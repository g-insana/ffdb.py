#!/usr/bin/env python3
"""
indexff
objective: generalized positional indexer for entries in a flatfile

by Giuseppe Insana, 2020
"""

# pylint: disable=C0103,R0912,R0915,W0603

import os
import sys
import re
import time
import getpass
import argparse
from heapq import merge #mergesort for merging at the end of multithread
from multiprocessing import Pool, Array #for multithreaded
from random import randint #for compressed flatfiles via gztool
from tqdm import tqdm #for progress bar
from sortedcontainers import SortedList #if sorted output
from ffdb import eprint, derive_key, \
    get_cipher_type, format_indexes, check_iofiles, int_to_b64, \
    calculate_blocksize, siprefix2num, calculate_chunknum, \
    print_subfiles, delete_files, compute_split_positions, elapsed_time, \
    entry_generator, shift_index_file, buffered_readwrite, TEMPDIR, \
    postprocess_entry, PROGRESSBARCHARS

#CUSTOMIZATIONS:
#you can modify the default size of block (chunk) of flatfile
#to work on for parallel execution
MAXBLOCKSIZE = "50M" #50 Mb of flatfile in each chunk
MINBLOCKSIZE = "100k" #100k minimum each chunk

#CONSTANTS
PROGNAME = "indexer.py"
VERSION = "4.0"
AUTHOR = "Giuseppe Insana"
args = None
patterns = None
joinedpatterns = None


def check_args():
    """
    parse arguments and check for error conditions
    """
    global args, patterns, joinedpatterns
    usagetxt = """{0} -f FLATFILE -i 'PATTERN' [-e ENDPATTERN] >INDEXFILE
    [-f] : flatfile to index
    [-i] : regex pattern for the identifiers; also [-j], see examples below
    [-e] : pattern for end of entry. defaults to "^-$"
    see '{0} -h' for tweaks and optional modes
    \nnotes: If compression or encryption is requested, an output flatfile will
         be created, and the resulting index will refer to it.
       If the identifiers are a LOT and memory is an issue, you may wish to use
         [-u] option and sort the resulting index after it has been generated.\nexamples:
       {0} -i '^AC   (.+?);' -f uniprot.dat -e '^//$' >up.pac
       {0} -i '^AC   (.+?);' 'ID   (.+?);' -f [...]
         (multiple patterns can be specified)

       {0} -i '^AC   (.+?);' -j '^OX   NCBI_(Tax)ID=(\\d+) ' -f [...]
         (complex patterns made of multiple parts can be specified with [-j];
          -i and -j patterns can be used together)

       {0} -a -j '^DR   (.+?);( .+?);' -f [...]
       {0} -a -i '^AC   (\\S+?); ?(\\S+)?;? ?(\\S+)?;?' -f [...]
         (use [-a] option to find all instances and capture groups of
          the provided patterns, not just the first one)
    """.format(PROGNAME)
    parser = argparse.ArgumentParser(description='Create a positional index for any flatfile, \
                                     optionally compressing or encrypting its entries',
                                     usage=usagetxt)
    parser.add_argument('-f', '--file', dest='input_filename',
                        help="Filename of flatfile to be processed", required=True)
    parser.add_argument('-i', '--id', dest='patterns',
                        help="regexp pattern for identifier(s) to index", required=False,
                        type=str, nargs='+')
    parser.add_argument('-j', '--joinedid', dest='joinedpatterns',
                        help="regexp pattern for identifier(s) to index", required=False,
                        type=str, nargs='+')
    parser.add_argument('-e', '--endpattern', dest='terminator',
                        help="regexp pattern to identify the end of each entry. If unspecified \
                        it defaults to '^-$'", required=False)
    parser.add_argument('-a', '--allmatches', dest='allmatches', action='store_true',
                        help="find all instances of the identifier pattern, not just the first one \
                        (the default behaviour)", required=False)
    parser.add_argument('-v', '--verbose', dest='verbose', action='store_true',
                        help="verbose operation", required=False)
    parser.add_argument('-t', '--threads', dest='threads',
                        help="use specified number of threads for parallel indexing",
                        required=False, type=int)
    parser.add_argument('-b', '--blocksize', dest='input_blocksize',
                        help="redefine blocksize used for parallel execution. By default \
                        it will be adjusted automatically to the number of threads",
                        required=False, type=siprefix2num)
    parser.add_argument('-o', '--offset', dest='pos_offset',
                        help="optional offset (in bytes) to shift entry positions in index",
                        required=False, type=int)
    parser.add_argument('-k', '--keysize', dest='keysize',
                        help="request entries to be encrypted and specify encryption strength: \
                        16=aes-128, 24=aes-192 or 32=aes-256. INPUT_FILENAME.enc will be created",
                        required=False, type=int, choices=(16, 24, 32))
    parser.add_argument('-p', '--passphrase', dest='passphrase',
                        help="passphrase for encrypting the entries; if unspecified it will be \
                        requested interactively (safer)", required=False, type=str)
    parser.add_argument('-c', '--compresslevel', dest='compresslevel',
                        help="request entries to be compressed and specify a compress level. \
                        INPUT_FILENAME.xz will be created",
                        required=False, type=int, choices=(0, 1, 2, 3, 4, 5, 6, 7, 8, 9))
    parser.add_argument('-x', '--xsanity', dest='xsanity', action='store_true',
                        help="compute entry checksums and add them to index",
                        required=False)
    parser.add_argument('-u', '--unsorted', dest='unsorted', action='store_true',
                        help="do not sort the index, leaving that task to a followup external \
                        command. Note that extraction requires a sorted index",
                        required=False)
    parser.add_argument('-n', '--nopos', dest='nopos', action='store_true',
                        help="do not compute positions, just print matching identifiers",
                        required=False)
    args = parser.parse_args()

    randnum = str(randint(1000, 9999))

    if args.patterns is None and args.joinedpatterns is None:
        eprint("    => ERROR: at least one of -i or -j needs to be given!")
        sys.exit(22)

    if args.patterns is None:
        args.patterns = []

    if args.joinedpatterns is None:
        args.joinedpatterns = []

    args.progressbar = False
    if args.verbose:
        eprint(" .-- {} v{} -- by {} --.".format(PROGNAME, VERSION, AUTHOR))
        args.progressbar = True

    if args.allmatches:
        if args.verbose:
            eprint(" |-- All matches of the pattern will be stored as identifiers")

    if args.nopos:
        if args.pos_offset is not None or args.compresslevel is not None \
                or args.passphrase is not None or args.keysize is not None or \
                args.xsanity:
            eprint("    => ERROR: No sense specifying compression, encryption, sanity")
            eprint("    =>        or pos_offset when using --nopos option")
            sys.exit(22)
        if args.verbose:
            eprint(" |-- No positional index will be created. Only printing identifiers found")

    if args.pos_offset is None:
        args.pos_offset = 0
    else:
        if args.verbose:
            eprint(" |-- positions to be offset by: {}".format(args.pos_offset))

    if args.unsorted:
        if args.verbose:
            eprint("    => NOTICE: index will be printed unsorted as requested.")
            eprint("       Please sort index before using it for extraction")

    patterns = set()
    for pattern in args.patterns:
        if args.verbose:
            eprint(" |-- adding identifier pattern '{}'".format(pattern))
        patterns.add(re.compile(pattern.encode('UTF-8'), re.MULTILINE))
    joinedpatterns = set()
    for pattern in args.joinedpatterns:
        if args.verbose:
            eprint(" |-- adding joined identifier pattern '{}'".format(pattern))
        joinedpatterns.add(re.compile(pattern.encode('UTF-8'), re.MULTILINE))

    if args.terminator is None:
        args.terminator = "^-$" #default
    if args.verbose:
        eprint(" |-- entry terminator pattern set as '{}'".format(args.terminator))

    if args.xsanity and args.verbose:
        eprint(" |-- entry checksums will be computed and added to index")

    if args.keysize is not None:
        if args.passphrase is None:
            eprint(" |-- keysize specified, please provide a passphrase:")
            args.passphrase = getpass.getpass(prompt=" |>> Passphrase: ")

    args.encrypt = False
    args.cipher_type = None
    args.key = None
    if args.passphrase is not None:
        args.encrypt = True
        if args.keysize is None:
            args.keysize = 16 #default
        args.cipher_name, args.key = derive_key(args.passphrase, args.keysize)
        args.cipher_type = get_cipher_type(args.cipher_name)
        if args.verbose:
            eprint(" |-- encrypted flatfile (with cipher {}) will be written to '{}.enc'".format(
                args.cipher_name, args.input_filename))
            #eprint(" |-- the passphrase is: {}".format(args.passphrase)) #DEBUG!
            #eprint(" |-- the encryption key is: {}".format(args.key)) #DEBUG!

    args.compress = False
    if args.compresslevel is not None:
        args.compress = True
        if args.encrypt:
            args.index_type = "+" #both compressed and encrypted
            if args.verbose:
                eprint(" |-- entries will be compressed and encrypted to .enc file")
        else:
            args.index_type = ":" #compressed but not encrypted
            if args.verbose:
                eprint(" |-- entries will be compressed to .xz file")
    else: #not compressed
        if args.encrypt:
            args.index_type = "." #encrypted but not compressed
            if args.verbose:
                eprint(" |-- entries will be encrypted to .enc file")
        else:
            args.index_type = "-" #not encrypted nor compressed but entry sizes stored

    if args.input_blocksize is not None:
        if args.threads is None:
            eprint("    => ERROR: specifying blocksize makes sense only for -t execution")
            sys.exit(22)
        if args.verbose:
            eprint(" |-- blocksize set to {} bytes".format(args.input_blocksize))
    if args.threads is not None: #multithread
        if args.threads < 2:
            eprint("    => ERROR: No sense specifying a number of threads lower than 2!")
            sys.exit(22)
        if args.input_blocksize is None:
            #if not specified, we use 1/threadnumTH of inputfilesize up
            #to a maximum MAXBLOCKSIZE and with minimum MINBLOCKSIZE
            args.input_blocksize = max(siprefix2num(MINBLOCKSIZE),
                                       min(
                                           calculate_blocksize(args.input_filename, args.threads),
                                           siprefix2num(MAXBLOCKSIZE)))
        args.input_filesize = os.path.getsize(args.input_filename)
        if args.input_blocksize > args.input_filesize // 2 * 3:
            eprint("    => NOTICE: blocksize too BIG compared to flatfile size, -t not applicable!")
            sys.exit(22)
        args.mt_subfiles_dir = TEMPDIR + "/tmpINDEX" + randnum + "/"
        args.mt_subfiles_fprefix = args.mt_subfiles_dir + "F"
        args.mt_subfiles_iprefix = args.mt_subfiles_dir + "I"
        args.mt_subfiles_oprefix = args.mt_subfiles_dir + "O"
        #find max number of chunks required (we'll adjust later on split)
        args.input_filesize, args.chunks_count = calculate_chunknum(args.input_filename,
                                                                    args.input_blocksize)
        if args.verbose:
            eprint(" |-- using maximum {} parallel threads (-t); your OS reports {} cpus.".format(
                args.threads, os.cpu_count()))
    else: # if unspecified, set args.threads to 1
        args.threads = 1
        args.chunks_count = 1

    if args.nopos:
        args.index_type = "" #no indexes


def check_files():
    """
    check for ability to open input/output filenames
    """
    check_iofiles([args.input_filename], [])

    args.outff_filename = None
    if args.encrypt:
        args.outff_filename = "{}.{}".format(args.input_filename, "enc")
    elif args.compress: #if compressed but not encrypted
        args.outff_filename = "{}.{}".format(args.input_filename, "xz")

    if args.outff_filename is not None:
        check_iofiles([], [args.outff_filename])

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
        #for chunknum in range(args.chunks_count): #DEBUG!
        #   eprint(" >> the temporary index file for chunk #{} will be '{}'".format(
        #       chunknum, args.chunk_itempfiles[chunknum]))

        #if outff needs to be generated
        if args.compress or args.encrypt:
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


def find_patterns_in_entry(entry, allmatches=False):
    """
    parse an entry, using regular expressions to capture desired fields
    """
    goodidentifiers = list()
    if allmatches:
        for pattern in patterns:
            identifiers = pattern.findall(entry['full'])
            #eprint("trying {} found {}".format(pattern, identifiers))
            for match in identifiers:
                if isinstance(match, tuple):
                    for submatch in match: #uglier but faster than list comprehension
                        if submatch:
                            goodidentifiers.append(submatch.decode())
                else:
                    goodidentifiers.append(match.decode())
        for pattern in joinedpatterns:
            identifiers = pattern.findall(entry['full'])
            #eprint("trying {} found {}".format(pattern, identifiers))
            for match in identifiers:
                if isinstance(match, tuple):
                    goodidentifiers.append(b''.join(match).decode())
                else:
                    goodidentifiers.append(match.decode())
    else:
        for pattern in patterns:
            match = pattern.search(entry['full'])
            if match:
                for submatch in match.groups():
                    if submatch:
                        goodidentifiers.append(submatch.decode())
        for pattern in joinedpatterns:
            match = pattern.search(entry['full'])
            if match:
                submatches = b''
                for submatch in match.groups():
                    if submatch:
                        submatches += submatch
                goodidentifiers.append(submatches.decode())

    entry['ids'].update(goodidentifiers)


def parse_ff_wrapper(chunknum):
    """
    wrapper to parse_ff for multithread use
    """
    #collect all index parameters in mode dict
    mode = {'encrypt': args.encrypt, 'compress': args.compress,
            'key': args.key, 'index_type': args.index_type,
            'cipher_type': args.cipher_type,
            'compresslevel': args.compresslevel,
            'terminator': args.terminator,
            'unsorted': args.unsorted,
            'xsanity': args.xsanity,
            'allmatches': args.allmatches,
            'nopos': args.nopos
            }

    #multithreaded will write each chunk index to a temporary file
    outindex_filename = None
    if args.threads > 1:
        outindex_filename = args.chunk_itempfiles[chunknum]

    #if encryption or compression, outfile will be generated
    outff_filename = None
    if args.threads > 1:
        if args.encrypt or args.compress:
            outff_filename = args.chunk_otempfiles[chunknum]
    else:
        outff_filename = args.outff_filename

    #compute pos_offset
    pos_offset = 0
    if args.threads > 1:
        input_file = args.chunk_ftemp_files[chunknum] #split chunk of flatfile
        buffered_readwrite(args.input_filename,
                           input_file,
                           args.chunk_ftemp_startpos[chunknum],
                           args.chunk_ftemp_filesizes[chunknum])

        pos_offset = args.pos_offset
        if chunknum > 0: #no need for first chunk
            if not args.compress: #if compress we'll shift indexes later
                for prev_chunknum in range(chunknum):
                    pos_offset += args.chunk_ftemp_filesizes[prev_chunknum]
            #eprint("#{}: adding prev chunks filesize, offset now {}".format( #DEBUG
            #   chunknum, pos_offset))
    else:
        input_file = args.input_filename
        pos_offset = args.pos_offset
    mode['pos_offset'] = pos_offset

    parse_ff_byentry(input_file,
                     mode,
                     outindex_filename=outindex_filename,
                     outff_filename=outff_filename,
                     chunknum=chunknum)


def parse_ff_byentry(inputfile, mode, outff_filename=None,
                     outindex_filename=None, chunknum=0):
    """
    parse input file entry by entry
    """
    global entries_count, indexes_count, skipped_count
    if not mode['unsorted']:
        mode['indexes'] = SortedList()
    #init entry hash
    entry = dict()
    entry['ids'] = set()
    entry['full'] = "" #used for calculating entrysizes, compressing or encrypting
    entry['length'] = 0
    if outff_filename is not None:
        mode['outffh'] = open(outff_filename, 'wb')
    if outindex_filename is None:
        mode['outindexfh'] = sys.stdout #by default print entries to stdout
    else:
        mode['outindexfh'] = open(outindex_filename, 'w')
    entryposition = 0 #position of first entry
    if mode['pos_offset'] > 0:
        entryposition = mode['pos_offset']
        #eprint("applying pos_offset of {}".format(pos_offset)) #DEBUG
    bufsize = 1024 * 1024
    for entry['full'], entry['length'] in entry_generator(inputfile, mode['terminator'], bufsize):
        entries_count[chunknum] += 1
        find_patterns_in_entry(entry, mode['allmatches'])
        if entry['ids']: #if identifiers found
            if not mode['nopos']:
                #position and postprocessing
                entry['position'] = int_to_b64(entryposition)
                postprocess_entry(entry, mode)

            #index formatting
            new_indexes = format_indexes(entry, mode['index_type'],
                                         mode['cipher_type'], mode['xsanity'])
            if mode['unsorted']: #write out continously the indexes
                mode['outindexfh'].write("".join(new_indexes))
                indexes_count[chunknum] += len(new_indexes)
            else: #if sorted, update sortedlist, write indexes in the end
                mode['indexes'].update(new_indexes)
            entry['ids'] = set() #reset entry hash
        else: #we skip the entry since we found no identifiers
            skipped_count[chunknum] += 1
        if not mode['nopos']:
            entryposition += entry['length'] # for next entry

    if not mode['unsorted']: #if sorted, we write all now, at the end
        indexes_count[chunknum] = len(mode['indexes'])
        for index in mode['indexes']:
            mode['outindexfh'].write(index)
    if outff_filename is not None:
        mode['outffh'].close()
    if outindex_filename is not None:
        mode['outindexfh'].close()


def print_stats(start_time):
    """
    if verbose print some final statistics on entries indexed
    """
    entries_sum = 0
    indexes_sum = 0
    skipped_sum = 0
    for chunknum in range(args.chunks_count):
        entries_sum += entries_count[chunknum]
        indexes_sum += indexes_count[chunknum]
        skipped_sum += skipped_count[chunknum]
    eprint(" '-- {} entries with {} indexes ~ {} entries skipped --'".format(
        entries_sum, indexes_sum, skipped_sum))
    eprint(" '-- Elapsed: {}, {} entries/sec --'".format(*elapsed_time(start_time, entries_sum)))


def init_thread(e, i, s):
    """
    to initialise multithreaded worker
    """
    global entries_count, indexes_count, skipped_count
    entries_count = e
    indexes_count = i
    skipped_count = s


if __name__ == '__main__':
    check_args()
    check_files()

    start_secs = time.time()
    if args.threads > 1: #multithread
        args.chunk_ftemp_files = list()

        #find out where to split the input file without breaking entries
        args.chunk_ftemp_startpos, args.chunk_ftemp_filesizes = compute_split_positions(
            args.input_filename, args.input_blocksize, args.terminator)
        args.chunks_count = len(args.chunk_ftemp_filesizes)
        suffixlength = len(str(args.chunks_count))
        for mychunknum in range(args.chunks_count):
            chunk_suffix = str(mychunknum).zfill(suffixlength)
            args.chunk_ftemp_files.append(args.mt_subfiles_dir + chunk_suffix)

        if args.verbose:
            eprint(" |-- parallel work in chunks of maximum {} bytes (-b to change)".format(
                args.input_blocksize))
            eprint(" |-- flatfile will be split into {} chunks".format(args.chunks_count))

        entries_count = Array('i', args.chunks_count)
        indexes_count = Array('i', args.chunks_count)
        skipped_count = Array('i', args.chunks_count)

        args.chunk_itempfiles = args.chunk_itempfiles[0:args.chunks_count]
        if args.outff_filename is not None:
            args.chunk_otempfiles = args.chunk_otempfiles[0:args.chunks_count]

        #init threads
        pool = Pool(args.threads, initializer=init_thread, initargs=(
            entries_count, indexes_count, skipped_count))
        #submit chunks to threads
        if args.progressbar:
            _ = list(tqdm(pool.imap(parse_ff_wrapper, range(args.chunks_count)),
                          total=args.chunks_count, ascii=PROGRESSBARCHARS))
        else:
            pool.imap(parse_ff_wrapper, range(args.chunks_count))
        pool.close() #no more work to submit
        pool.join() #wait workers to finish

        delete_files(args.chunk_ftemp_files) #cleanup

        #final union of results:
        if args.compress:
            otempfile_offset = 0
            otempfile_offsets = list()
            # 1) go through .enc or .xz files and sum filesizes as offsets
            for otempfile in args.chunk_otempfiles:
                otempfile_offsets.append(otempfile_offset)
                otempfile_offset += os.path.getsize(otempfile)

            # 2) go through indexes (a part first one) and shift their offset
            for mychunknum in range(args.chunks_count):
                itempfile = args.chunk_itempfiles[mychunknum]
                outfile = itempfile + "s"
                shift_index_file(itempfile,
                                 otempfile_offsets[mychunknum], outfile,
                                 args.index_type)
                args.chunk_itempfiles[mychunknum] = outfile #replace
                os.remove(itempfile)

        #print merged index
        if args.unsorted: #simple printout
            print_subfiles(args.chunk_itempfiles)
        else: #merge sort and printout
            indexfilehandles = list()
            for filename in args.chunk_itempfiles:
                ifh = open(filename, 'r', 1)
                indexfilehandles.append(ifh)
            sys.stdout.writelines(merge(*indexfilehandles)) #heapq mergesort
        delete_files(args.chunk_itempfiles) #cleanup

        #concatenate final encrypted or compressed filefile, if requested
        if args.outff_filename is not None:
            with open(args.outff_filename, 'wb') as outputfh:
                print_subfiles(args.chunk_otempfiles, outputfh)
            delete_files(args.chunk_otempfiles) #cleanup

        if os.path.exists(args.mt_subfiles_dir):
            os.rmdir(args.mt_subfiles_dir) #cleanup
    else: #singlethread
        entries_count = Array('i', 1)
        indexes_count = Array('i', 1)
        skipped_count = Array('i', 1)

        parse_ff_wrapper(0)

    #final printout
    print_stats(start_secs)
