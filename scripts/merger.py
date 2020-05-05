#!/usr/bin/env python3
"""
merger.py
objective: merge new entries to existing flatfile, updating its index

by Giuseppe Insana, 2020
"""

# pylint: disable=C0103,R0912,R0915,W0603

#IMPORTS:
import os
import sys
import argparse
from random import randint
from shutil import copyfile
from ffdb import eprint, check_index, delete_files, GZTOOL_EXE, \
    merge_indexes, TEMPDIR, BUFFERSIZE

#CONSTANTS
PROGNAME = "merger.py"
VERSION = "1.5"
AUTHOR = "Giuseppe Insana"
args = None


def check_index_compatibility(indexfile1, indexfile2):
    """
    will check if old and new indexes are of same type
    """

    with open(indexfile1, 'r', 1) as indexfh:
        line1 = indexfh.readline() #first line of index

    index_type1, cipher1, keysize1, has_checksum1 = check_index(line1)

    with open(indexfile2, 'r', 1) as indexfh:
        line2 = indexfh.readline() #first line of index

    index_type2, cipher2, keysize2, has_checksum2 = check_index(line2)

    if index_type1 != index_type2 or cipher1 != cipher2 or keysize1 != keysize2:
        eprint("    => ERROR: The indexes you are merging '{}' and '{}'".format(
            indexfile1, indexfile2))
        eprint("              are of incompatible type! Cannot proceed!")
        sys.exit(22)
    if has_checksum1 != has_checksum2:
        eprint("    => ERROR: The indexes you are merging '{}' and '{}'".format(
            indexfile1, indexfile2))
        eprint("              are of the same type but only one contains checksums!")
        eprint("       Both should be either with or without checksums. Cannot proceed!")
        sys.exit(22)
    args.index_type = index_type1


def uncompress_file(filename):
    """
    uncompress given filename
    return name of the uncompressed file
    """
    return filename


def compress_file(filename, create_gzindex=None):
    """
    compress given filename
    optionally create gzindex
    return name of the uncompressed file
    """
    if create_gzindex is not None:
        #open pipe to call external gztool
        pass
    return filename


def concatenate_into(filename, addenda, outfilename):
    """
    first copy original file to a new file
    then append content of addenda file at the end of the new file
    """
    copyfile(filename, outfilename)
    #append_to(outfilename, addenda) #reads the whole file in memory
    append_to_buffered(outfilename, addenda)


def append_to_buffered(filename, addenda):
    """
    append addenda file at the end of the given file, using buffered read/write
    """
    with open(addenda, 'rb') as inputfh:
        with open(filename, 'ab') as outfh:
            ##straight, unbuffered, will read whole file in memory:
            #outfh.write(inputfh.read())
            ##buffered:
            blockcount, remainder = divmod(os.path.getsize(addenda), BUFFERSIZE)
            for _ in range(blockcount):
                buffered = inputfh.read(BUFFERSIZE)
                outfh.write(buffered)
            outfh.write(inputfh.read(remainder))
            outfh.flush()


def check_args():
    """
    parse arguments and check for error conditions
    """
    global args, GZTOOL_EXE
    usagetxt = """{0} -f FLATFILE -i INDEXFILE -e ENTRIESFILE -n NEWINDEXFILE
    [-f] : flatfile into which the new entries should be added
    [-i] : index of FLATFILE
    [-e] : filename containing the new entries to be added
    [-n] : index of ENTRIESFILE
    see {0} -h for tweaks and optional modes
    \nexamples:
       {0} -f db.dat -i db.pos -e new.dat -n new.pos
         (will update db.dat and db.pos)
       {0} -c -f db.dat -i db.pos -e new.dat -n new.pos
         (will create db.dat.new and db.pos.new)
       {0} -c -o export -f db.dat -i db.pos -e new.dat -n new.pos
         (will create export/db.dat.new and export/db.pos.new)
    """.format(PROGNAME)
    parser = argparse.ArgumentParser(description='Merge new pre-indexed entries into an existing \
                                     flatfile', usage=usagetxt)
    parser.add_argument('-f', '--file', dest='ff_filename',
                        help="filename of flatfile to be processed",
                        required=True, type=str)
    parser.add_argument('-i', '--index', dest='index_filename',
                        help="filename of index file with entry identifiers",
                        required=True, type=str)
    parser.add_argument('-e', '--entries', dest='newentries_filename',
                        help="filename of new entries to be merged into flatfile",
                        required=True, type=str)
    parser.add_argument('-n', '--newindex', dest='newindex_filename',
                        help="filename of index file with entry identifiers",
                        required=True, type=str)
    parser.add_argument('-c', '--create', dest='createmode', action='store_true',
                        help="create new files (.new extension) rather than updating existing \
                        files (the default operation mode)", required=False)
    parser.add_argument('-o', '--outpath', dest='outpath',
                        help="optionally write new files to specified path rather than creating \
                        new files in the same location as the original ones",
                        required=False, type=str)
    parser.add_argument('-v', '--verbose', dest='verbose', action='store_true',
                        help="verbose operation", required=False)
    parser.add_argument('-d', '--delete', dest='deleteafter', action='store_true',
                        help="delete ENTRIESFILE and NEWINDEXFILE after merging is completed",
                        required=False)
    parser.add_argument('-g', '--gzip', dest='gzip', action='store_true',
                        help="compress the final flatfile after merge, creating .gzi compressed \
                        index", required=False)
    parser.add_argument('-s', '--small', dest='smallnew', action='store_true',
                        help="use this mode if the new index is small (<30k entries): performance \
                        should be better", required=False)
    args = parser.parse_args()

    if args.verbose:
        eprint(" .-- {} v{} -- by {} --.".format(PROGNAME, VERSION, AUTHOR))

    args.ff_compressed = False
    if args.ff_filename[-3:] == ".gz":
        args.ff_compressed = True
        if not args.gzip:
            eprint("    => NOTICE: -f argument has extension .gz: assuming flatfile is compressed")
            eprint("               it will be uncompressed and then recompressed after merge")
            args.gzip = True

    args.newentries_compressed = False
    if args.newentries_filename[-3:] == ".gz":
        args.newentries_compressed = True
        eprint("    => NOTICE: -n argument has extension .gz: assuming newentriesfile compressed")

    if args.ff_filename.find("://") != -1:
        eprint("    => ERROR: {} cannot operate on remote flatfiles".format(PROGNAME))
        sys.exit(22)

    if args.smallnew:
        if args.verbose:
            eprint(" |-- using tweak for smaller new index")
    else:
        randnum = str(randint(1000, 9999))
        args.itemp_filename = TEMPDIR + "/tmpMERGE" + randnum

    if args.outpath is None:
        args.outindex_filename = args.index_filename + ".new"
    else:
        if not os.access(args.outpath, os.W_OK):
            eprint("    => ERROR: specified outpath '{}' doesn't exist or is not writable!".format(
                args.outpath
            ))
            sys.exit(1)
        args.outindex_filename = os.path.join(args.outpath, args.index_filename + ".new")


def check_iofiles(read_filenames, write_filenames):
    """
    check for ability to open input/output filenames
    """
    if read_filenames is not None:
        for filename in read_filenames:
            try:
                inputfh = open(filename, 'r')
                inputfh.close()
            except FileNotFoundError:
                eprint("    => ERROR: Cannot open file '{}' for reading".format(filename))
                sys.exit(2)

    if write_filenames is not None:
        for filename in write_filenames:
            if os.path.isfile(filename):
                eprint("    => ERROR: file '{}' exists".format(filename))
                eprint("       please remove it as we refuse to overwrite it!")
                sys.exit(1)
            try:
                myoutputfh = open(filename, 'w')
                myoutputfh.close()
            except PermissionError:
                eprint("    => ERROR: Cannot open file '{}' for writing".format(filename))
                sys.exit(1)
            #eprint("deleting {}".format(filename)) #DEBUG
            delete_files([filename])


def check_files():
    """
    check for ability to open input/output filenames
    """
    check_iofiles([args.ff_filename, args.newentries_filename, args.index_filename,
                   args.newindex_filename], [args.outindex_filename])

    if not args.smallnew:
        check_iofiles(None, [args.itemp_filename])

    if args.createmode:
        if args.outpath is None:
            args.outff_filename = args.ff_filename + ".new"
        else:
            args.outff_filename = os.path.join(args.outpath, args.ff_filename + ".new")
        check_iofiles(None, [args.outff_filename])


if __name__ == '__main__':
    #parse and check arguments
    check_args()

    #check files (if they can be read/written)
    check_files()

    #check if old and new indexes are of same type:
    check_index_compatibility(args.index_filename, args.newindex_filename)

    #uncompress files if needed
    if args.ff_compressed:
        if args.verbose:
            eprint(" |-- uncompressing original flatfile.. this may take some time..")
        args.ff_filename = uncompress_file(args.ff_filename)
    if args.newentries_compressed:
        if args.verbose:
            eprint(" |-- uncompressing newentries file.. this may take some time..")
        args.newentries_filename = uncompress_file(args.newentries_filename)

    #calculate index offset
    pos_offset = os.path.getsize(args.ff_filename)

    #merge old and new identifiers' indexes
    if args.smallnew:
        tempfile = None
    else:
        tempfile = args.itemp_filename
    new_identifiers_count = merge_indexes(args.index_filename, args.newindex_filename,
                                          pos_offset, args.index_type,
                                          args.outindex_filename, tempfile)

    #create new flatfile or update original flatfile
    if args.createmode:
        if args.verbose:
            eprint(" |-- new indexfile created: '{}'".format(args.outindex_filename))
        concatenate_into(args.ff_filename, args.newentries_filename, args.outff_filename)
        if args.gzip: #compress flatfile
            if args.verbose:
                eprint(" |-- compressing new flatfile.. this may take some time..")
            args.outff_filename = compress_file(args.outff_filename, create_gzindex=True)
            if args.verbose:
                eprint(" |-- new compressed flatfile created: '{}'".format(args.outff_filename))
        else:
            if args.verbose:
                eprint(" |-- new flatfile created: '{}'".format(args.outff_filename))
    else: #updatemode
        os.replace(args.outindex_filename, args.index_filename)
        if args.verbose:
            eprint(" '-- original indexfile updated: '{}'".format(args.index_filename))
        append_to_buffered(args.ff_filename, args.newentries_filename)
        if args.gzip: #compress flatfile
            if args.verbose:
                eprint(" |-- compressing updated flatfile.. this may take some time..")
            args.ff_filename = compress_file(args.ff_filename, create_gzindex=True)
            if args.verbose:
                eprint(" |-- original flatfile updated and compressed: '{}'".format(
                    args.ff_filename))
        else:
            if args.verbose:
                eprint(" |-- original flatfile updated: '{}'".format(args.ff_filename))

    #final cleanup, if desired
    if args.deleteafter:
        delete_files([args.newentries_filename, args.newindex_filename])

    #final printout
    eprint(" '-- merged {} new entries".format(new_identifiers_count))
