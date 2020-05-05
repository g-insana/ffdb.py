#!/usr/bin/env python3
"""
remover
objective: generalized entry remover from local flatfile, it deletes entries and reindexes

by Giuseppe Insana, 2020
"""

# pylint: disable=C0103,R0912,R0915,W0603

#IMPORTS:
import os
import sys
import time
import argparse
from random import randint
from multiprocessing import Pool, Array
from sortedcontainers import SortedList
from tqdm import tqdm #progress bar
from ffdb import eprint, check_iofiles, b64_to_int, int_to_b64, \
    print_subfiles, elapsed_time, TEMPDIR, siprefix2num, calculate_chunknum, \
    calculate_blocksize, check_index, FIELDSEPARATOR, REESIVN, REES, BUFFERSIZE, \
    get_position_first, get_position_last, get_positions, delete_files, split_file, \
    PROGRESSBARCHARS

#CUSTOMIZATIONS:
#you can modify the minimum default size of block (chunk) of index identifiers
#to work on for parallel execution
MINBLOCKSIZE = "40k" #40 kb of index identifiers per chunk

#CONSTANTS
PROGNAME = "remover.py"
VERSION = "1.2"
AUTHOR = "Giuseppe Insana"
args = None


def check_args():
    """
    parse arguments and check for error conditions
    """
    global args
    usagetxt = """{0} -f FLATFILE -i INDEXFILE -l LISTFILE [-o OUTPATH]
    [-f] : flatfile from which the entries should be removed
    [-i] : index of FLATFILE
    [-l] : file with list of identifers for the entries that should be removed
    see {0} -h for tweaks and optional modes
    \nexamples:
    {0} -f entries.dat -i entries.pos -l removeme.list
      (will create entries.dat.new and entries.pos.new)
    {0} -f entries.dat -i entries.pos -l removeme.list -o cleaned
      (will create cleaned/entries.dat.new and cleaned/entries.pos.new)
    """.format(PROGNAME)
    parser = argparse.ArgumentParser(description='Use a positional index to delete \
                                     entries from a flatfile', usage=usagetxt)
    parser.add_argument('-f', '--file', dest='flatfile',
                        help="filename of flatfile from which entries should be deleted",
                        required=True, type=str)
    parser.add_argument('-i', '--index', dest='index_filename',
                        help="filename of index file containing entry identifiers",
                        required=True, type=str)
    parser.add_argument('-l', '--list', dest='list_filename',
                        help="a file containing a list of identifiers corresponding to entries \
                        to delete", required=True, type=str)
    parser.add_argument('-o', '--outpath', dest='outpath',
                        help="write new files to specified path rather than creating \
                        new files in the same location as the original ones",
                        required=False, type=str)
    parser.add_argument('-v', '--verbose', dest='verbose', action='store_true',
                        help="verbose operation", required=False)
    parser.add_argument('-d', '--duplicates', dest='duplicates', action='store_true',
                        help="specify INDEX_FILE could contain duplicate identifiers and request \
                        deletion of all of them (default is to delete a single entry)",
                        required=False)
    parser.add_argument('-z', '--zfound', dest='zfound', action='store_true',
                        help="specify INDEX_FILE contains duplicate identifiers and request \
                        deletion of last entry appearing in the flatfile (default is the first)",
                        required=False)
    parser.add_argument('-t', '--threads', dest='threads',
                        help="use specified number of multiple threads for parallel reindexing",
                        required=False, type=int)
    parser.add_argument('-b', '--blocksize', dest='index_blocksize',
                        help="redefine blocksize used for parallel execution. By default \
                        it will be adjusted automatically to the number of threads",
                        required=False, type=siprefix2num)
    args = parser.parse_args()

    randnum = str(randint(1000, 9999))

    args.progressbar = False
    if args.verbose:
        eprint(" .-- {} v{} -- by {} --.".format(PROGNAME, VERSION, AUTHOR))
        args.progressbar = True

    if args.zfound and args.duplicates:
        eprint("    => ERROR: No sense specifying both --zfound and --duplicates at the same time")

    if args.verbose:
        if args.zfound:
            eprint(" |-- [-z] option selected:")
            eprint(" |   if duplicates in index, the entry appearing last in ff will be deleted")
        elif args.duplicates:
            eprint(" |-- [-d] option selected:")
            eprint(" |-- if duplicates in index, all corresponding entries in ff will be deleted")
        else:
            eprint(" |-- if duplicates in index, the entry appearing first in ff will be deleted")
            eprint(" |   you can change this behaviour with [-z] or [-d]")

    if args.flatfile[-3:] == ".gz":
        eprint("    => ERROR: -f argument has extension .gz; wrong file??")
        sys.exit(22)

    if args.flatfile.find("://") != -1 and not args.remote:
        eprint("    => NOTICE: -f argument appears to be an URL; wrong file??")
        sys.exit(22)

    if args.outpath is None:
        args.outindex_filename = args.index_filename + ".new"
        args.output_filename = args.flatfile + ".new"
    else:
        if not os.access(args.outpath, os.W_OK):
            eprint("    => ERROR: specified outpath '{}' doesn't exist or is not writable!".format(
                args.outpath))
            sys.exit(1)
        args.outindex_filename = os.path.join(args.outpath, args.index_filename + ".new")
        args.output_filename = os.path.join(args.outpath, args.flatfile + ".new")
    if args.verbose:
        eprint(" |-- updated flatfile and index will be '{}' '{}'".format(
            args.output_filename,
            args.outindex_filename))

    #gather information from first line of index
    args.encrypted = False
    with open(args.index_filename, 'r', 1) as indexfh:
        args.index_type, _, _, _ = check_index(indexfh.readline())
    if args.index_type in (".", "+"):
        args.encrypted = True
        if args.verbose:
            eprint(" |-- index made for encrypted entries")
            eprint(" `=> Please ensure the -f filename points to the encrypted flatfile")
    if args.index_type in (":", "+"):
        if args.verbose:
            eprint(" |-- index made for compressed entries")
            eprint(" `=> Please ensure the -f filename points to the compressed flatfile")

    if args.index_blocksize is not None:
        if args.threads is None:
            eprint("    => ERROR: specifying blocksize makes sense only for -t execution")
            sys.exit(22)
        if args.verbose:
            eprint(" |-- blocksize set to {} bytes".format(args.index_blocksize))
    if args.threads is not None: #multithread
        if args.threads < 2:
            eprint("    => ERROR: No sense specifying a number of threads lower than 2!")
            sys.exit(22)
        if args.index_blocksize is None:
            #if not specified, we use 1/threadnumTH of filesize up to a minimum MINBLOCKSIZE
            args.index_blocksize = max(
                calculate_blocksize(args.index_filename, args.threads),
                siprefix2num(MINBLOCKSIZE))
        args.mt_subfiles_dir = TEMPDIR + "/tmpREINDEX" + randnum + "/"
        args.mt_subfiles_iprefix = args.mt_subfiles_dir + "I"
        args.mt_subfiles_oprefix = args.mt_subfiles_dir + "O"
        args.list_filesize, args.chunks_count = calculate_chunknum(
            args.index_filename, args.index_blocksize)
        if args.verbose:
            eprint(" |-- Parallel work in {} chunks of maximum {} bytes (-b to change)".format(
                args.chunks_count, args.index_blocksize))
            eprint(" |-- using maximum {} parallel threads (-t); your OS reports {} cpus.".format(
                args.threads, os.cpu_count()))
    else: # if unspecified, set args.threads to 1
        args.threads = 1
        args.chunks_count = 1


def check_files():
    """
    do some checks on availability of resources
    """
    check_iofiles([args.flatfile], [])
    check_iofiles([], [args.output_filename, args.outindex_filename])
    if args.threads > 1: #multithread
        os.mkdir(args.mt_subfiles_dir, mode=0o700)
        args.chunk_itempfiles = list()
        for chunknum in range(args.chunks_count):
            chunknumstr = str(chunknum).zfill(len(str(args.chunks_count))) #e.g. 00, 01..
            chunk_tempfile = args.mt_subfiles_iprefix + "." + chunknumstr
            try:
                myoutputfh = open(chunk_tempfile, 'w')
            except PermissionError:
                eprint("    => ERROR: Cannot open temporary file '{}' for writing".format(
                    chunk_tempfile))
                sys.exit(1)
            args.chunk_itempfiles.append(chunk_tempfile) #store temp filenames
            myoutputfh.close()
        delete_files(args.chunk_itempfiles)

        args.chunk_otempfiles = list()
        for chunknum in range(args.chunks_count):
            chunknumstr = str(chunknum).zfill(len(str(args.chunks_count))) #e.g. 00, 01..
            chunk_tempfile = args.mt_subfiles_oprefix + "." + chunknumstr
            try:
                myoutputfh = open(chunk_tempfile, 'w')
            except PermissionError:
                eprint("    => ERROR: Cannot open temporary file '{}' for writing".format(
                    chunk_tempfile))
                sys.exit(1)
            args.chunk_otempfiles.append(chunk_tempfile) #store temp filenames
            myoutputfh.close()
        delete_files(args.chunk_otempfiles)


def collect_entry(indexfh, identifier):
    """
    find position and size of entries corresponding to specified identifier
    from index
    """
    if args.duplicates:
        positions = get_positions(indexfh, identifier)
    elif args.zfound:
        positions = [get_position_last(indexfh, identifier)]
    else:
        positions = [get_position_first(indexfh, identifier)]

    if not positions or positions == [None]: #empty array
        if args.verbose:
            eprint("    => WARNING: '{}' not found in index; skipping".format(identifier))
        return [], []

    entry_positions = list()
    entry_lengths = list()

    for position in positions:
        #1) extract position and entry size
        if args.encrypted: #with iv
            posmatch = REESIVN.match(position)
        else: #no iv
            posmatch = REES.match(position)
        entry_position, entry_length = posmatch.groups()

        #2) decode and append to lists
        entry_positions.append(b64_to_int(entry_position))
        entry_lengths.append(b64_to_int(entry_length))

    return entry_positions, entry_lengths


def collect_entries_to_delete(list_filename, index_filename):
    """
    collect information about entries to delete, consulting the index,
    return a sorted list and a dict
    """
    indexfh = open(index_filename, 'r', 1)

    all_entries_positions = SortedList()
    all_entries_position2size = dict()

    identifiers_count = 0
    with open(index_filename) as indexfh:
        with open(list_filename) as listfh:
            for line in listfh:
                identifier = line.rstrip()
                identifiers_count += 1
                entry_positions, entry_sizes = collect_entry(indexfh, identifier)
                if entry_positions:
                    #eprint("got positions {} and sizes {} for identifier {}".format(
                    #   entry_positions, entry_sizes, identifier)) #debug
                    all_entries_positions.update(entry_positions) #add to sorted list
                    for entry_position, entry_size in zip(entry_positions, entry_sizes):
                        all_entries_position2size[entry_position] = entry_size #update dict

    return all_entries_positions, all_entries_position2size, identifiers_count


def delete_entries(inputfile, outputfile, sorted_positions, position2size):
    """
    delete the entries from input_filename writing into output_filename
    according to given positions and sizes
    """
    current_position = 0 #start from beginning
    with open(inputfile, 'rb') as inputfh:
        with open(outputfile, 'wb') as outfh:
            for entry_position in sorted_positions:
                entry_size = position2size[entry_position]
                write_size = entry_position - current_position
                if write_size > 0: #no sense writing nothing
                    inputfh.seek(current_position)
                    #eprint("  .. writing {} bytes from {}".format( #debug
                    #   write_size, current_position)) #debug
                    #buffered write:
                    blockcount, remainder = divmod(write_size, BUFFERSIZE)
                    for _ in range(blockcount):
                        buffered = inputfh.read(BUFFERSIZE)
                        outfh.write(buffered)
                    outfh.write(inputfh.read(remainder))
                current_position += write_size + entry_size #skip to after the entry being deleted

            #write the last piece of inputfile after last entry
            write_size = os.path.getsize(inputfile) - current_position
            if write_size > 0: #no sense writing nothing
                inputfh.seek(current_position)
                #eprint("  .. writing {} bytes from {}".format( #debug
                #   write_size, current_position)) #debug
                #buffered write:
                blockcount, remainder = divmod(write_size, BUFFERSIZE)
                for _ in range(blockcount):
                    buffered = inputfh.read(BUFFERSIZE)
                    outfh.write(buffered)
                outfh.write(inputfh.read(remainder))
            current_position += entry_size #skip to after the entry being deleted

            outfh.flush()


def find_shift_offset(position, sorted_positions, position2offset):
    """
    Find the offset to apply to a position,
    depending on cumulative sizes of entries deleted before it
    """
    first_deleted_position = sorted_positions[0]
    offset = 0
    if position < first_deleted_position:
        #quick heuristic: no need to check if position lower than first deleted
        #eprint("{}: before any deletions, no change".format(position)) #DEBUG
        return offset
    for deleted_position in reversed(sorted_positions):
        if position >= deleted_position:
            offset = position2offset[deleted_position]
            #eprint("{} >= {} so should decrement by {}".format( #debug
            #   position, deleted_position, offset)) #debug
            break
    #if offset == 0: #debug
    #   eprint("{}: no change".format(position)) #debug
    return offset


def update_index_after_deletions(index_filename, outfile,
                                 sorted_positions, position2offset,
                                 index_type):
    """
    update index file, creating outindex with entry positions shifted according to
    information given
    """
    #go through index and shift positions according to dict with offsets
    #for how much to shift entries found after a specified position
    buffered = ""
    all_indexes_count = 0
    skipped_count = 0
    shifted_count = 0
    unshifted_count = 0
    buflines = 100 #write every 100 lines
    bufcounter = 0
    with open(index_filename, 'r', 1) as indexfh: #using line buffering
        with open(outfile, 'w') as outfh:
            for line in indexfh:
                columns = line.rstrip().split(FIELDSEPARATOR)
                position = columns[1] #2nd column of index file is position
                if index_type in ("+", "."): #entry sizes and iv stored
                    posmatch = REESIVN.match(position)
                elif index_type in (":", "-"): #entry sizes stored, no iv
                    posmatch = REES.match(position)
                position, remaining = posmatch.groups()
                position = b64_to_int(position)
                if position in position2offset:
                    skipped_count += 1
                    continue
                if bufcounter > buflines: #write to outfile and reset buffer
                    outfh.write(buffered)
                    all_indexes_count += bufcounter
                    bufcounter = 0
                    buffered = ""

                offset = find_shift_offset(position, sorted_positions, position2offset)
                if offset > 0:
                    columns[1] = "{}{}{}".format(int_to_b64(position - offset),
                                                 index_type,
                                                 remaining)
                else:
                    unshifted_count += 1
                bufcounter += 1
                buffered += FIELDSEPARATOR.join(columns) + "\n"
            all_indexes_count += bufcounter
            outfh.write(buffered) #last block
            shifted_count = all_indexes_count - unshifted_count
            all_indexes_count += skipped_count
    return all_indexes_count, shifted_count, skipped_count


def print_stats(start_time):
    """
    print some final statistics on entries deleted
    """
    indexes_sum = 0
    deleted_sum = 0
    reindexed_sum = 0
    for chunknum in range(args.chunks_count):
        indexes_sum += indexes_count[chunknum]
        reindexed_sum += reindexed_count[chunknum]
        deleted_sum += deleted_count[chunknum]

    if found_count == 1:
        eprint(" |-- Found and removed 1 entry.")
    elif found_count > 0:
        eprint(" |-- Found and removed {} entries.".format(
            found_count))
    if found_count < requested_count:
        if found_count == 0:
            eprint("    => WARNING: NONE of the {} requested identifiers found in index!".format(
                requested_count))
        else:
            eprint("    => WARNING: only {} of the {} requested identifiers found in index.".format(
                found_count, requested_count))
    eprint(" |-- Deleted {} and reindexed {} indexes out of total {}.".format(
        deleted_sum, reindexed_sum, indexes_sum))
    _, reindexed_speed = elapsed_time(start_time, reindexed_sum)
    eprint(" '-- Elapsed: {}, {} deletions/sec; {} reindexing/sec --'".format(
        *elapsed_time(start_time, found_count), reindexed_speed))


def update_index_wrapper(chunknum):
    """
    wrapper to shift index for multithread use
    """
    #global reindexed_count, deleted_count
    if args.threads > 1:
        index_filename = args.chunk_itempfiles[chunknum]
        outindex_filename = args.chunk_otempfiles[chunknum]
    else:
        index_filename = args.index_filename
        outindex_filename = args.outindex_filename

    indexes_count[chunknum], reindexed_count[chunknum], \
        deleted_count[chunknum] = update_index_after_deletions(
            index_filename, outindex_filename, mysorted_positions,
            myposition2offset, args.index_type)


def init_thread(i, r, d):
    """
    to initialise multithreaded worker
    """
    global indexes_count, reindexed_count, deleted_count
    indexes_count = i
    reindexed_count = r
    deleted_count = d


if __name__ == '__main__':
    check_args()
    check_files()

    if args.output_filename is None:
        args.outputfh = sys.stdout
    else:
        args.outputfh = open(args.output_filename, 'wb')

    start_secs = time.time()

    #1) collect information from index on entries to delete
    mysorted_positions, myposition2size, \
        requested_count = collect_entries_to_delete(args.list_filename, args.index_filename)
    found_count = len(mysorted_positions)

    #2) delete entries from flatfile
    delete_entries(args.flatfile, args.output_filename, mysorted_positions, myposition2size)

    #3) fill dict with cumulative offsets, used to update index
    myposition2offset = dict()
    size_offset = 0
    for myentry_position in mysorted_positions:
        size_offset += myposition2size[myentry_position]
        myposition2offset[myentry_position] = size_offset
    #eprint("removed a total of {} bytes".format(size_offset)) #debug
    if os.path.getsize(args.flatfile) - size_offset != os.path.getsize(args.output_filename):
        eprint("    => ERROR: problems with deletion, file size of resulting file is wrong")
        sys.exit(1)

    #4) update the index shifting positions, optionally with multithread
    if args.threads > 1: #multithread
        args.chunk_itempfiles, _ = split_file(args.index_filename,
                                              args.index_blocksize,
                                              args.mt_subfiles_iprefix)
        args.chunks_count = len(args.chunk_itempfiles)

        if args.verbose:
            eprint(" |-- parallel reindexing in chunks of maximum {} bytes (-b to change)".format(
                args.index_blocksize))
            eprint(" |-- index will be split into {} chunks".format(args.chunks_count))

        indexes_count = Array('i', [0] * args.chunks_count)
        reindexed_count = Array('i', [0] * args.chunks_count)
        deleted_count = Array('i', [0] * args.chunks_count)

        args.chunk_otempfiles = args.chunk_otempfiles[0:args.chunks_count]

        #init threads
        pool = Pool(args.threads, initializer=init_thread,
                    initargs=(indexes_count, reindexed_count, deleted_count))
        #submit chunks to threads
        if args.progressbar:
            _ = list(tqdm(pool.imap(update_index_wrapper, range(args.chunks_count)),
                          total=args.chunks_count, ascii=PROGRESSBARCHARS))
        else:
            pool.imap(update_index_wrapper, range(args.chunks_count))
        pool.close() #no more work to submit
        pool.join() #wait workers to finish

        delete_files(args.chunk_itempfiles) #cleanup

        #final union of results: combine shifted index chunks
        with open(args.outindex_filename, 'wb') as outputfh:
            print_subfiles(args.chunk_otempfiles, outputfh)

        delete_files(args.chunk_otempfiles) #cleanup
    else:
        indexes_count = [0]
        deleted_count = [0]
        reindexed_count = [0]
        update_index_wrapper(0)

    print_stats(start_secs)
