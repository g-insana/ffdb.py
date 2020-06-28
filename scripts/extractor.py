#!/usr/bin/env python3
"""
extractor
objective: generalized entry extractor from local/remote/compressed/encrypted flatfile

by Giuseppe Insana, 2020
"""

# pylint: disable=C0103,R0912,R0915,W0603

#IMPORTS:
import os
import re
import sys
import time
import zlib
import getpass
import argparse
from math import ceil
from random import randint
from sortedcontainers import SortedList #if sorted output
#OPTIONAL IMPORTS (uncomment any of the following if never needed):
from subprocess import run, Popen, PIPE, DEVNULL #for compressed flatfiles via gztool
from multiprocessing import Pool, Array, current_process #for multithreaded
from tqdm import tqdm #for progress bar
from requests import get #for remote flatfiles via http
#NECESSARY IMPORTS
from ffdb import eprint, inflate, derive_key, check_index, \
    delete_files, REES, REESIV, GZTOOL_EXE, TEMPDIR, \
    init_cipher, check_iofiles, b64_to_int, \
    calculate_chunknum, calculate_blocksize, chunk_of_lines, siprefix2num, \
    read_from_size, print_subfiles, close_subfiles, elapsed_time, int_to_b64, \
    get_position_first, get_position_last, \
    get_position_checksum_first, get_position_checksum_last, \
    get_positions, get_positions_checksums, PROGRESSBARCHARS

#CUSTOMIZATIONS:
#you can modify the minimum default size of block (chunk) of identifiers list
#to work on for parallel execution
MINBLOCKSIZE = "40k" #500 kb of identifiers list per chunk

#you can specify that entries could be as big as (only used for caching files for remote extraction)
#if an entry is requested that is bigger than this specified maximum, the cache file will be
#downloaded again
MAXENTRYSIZE = 204800 #200kb

# for remote compressed flatfiles we apply a conservative (+40%) estimation
# (from reported average compress_ratio), i.e. we download more than strictly
# necessary, since the compress_ratio is an average and the particular chunk of
# the compressed file could have different compression ratio.
# If many entries fail to be extracted, considering raising the estimation
# (e.g. 5000 for +50%, 4000 for +60%..). Conversely to save bandwidth you may
# lower it (e.g. use 9000 for only +10% overdownload amount)
OVERDLFACTOR = 6000

# for remote compressed flatfiles when using cache we assume the default 10MiB span
# between index points on the gztool uncompressed stream (governed by -s option). If
# you are using a different span, modify the following value
GZCHUNKSPAN = 10485760 #10Mib

#CONSTANTS
RECOMPRESSRATIO = re.compile(r'^\tGuessed gzip.* \(([0-9.]+)%\) .*$', re.MULTILINE)
REGZINDEX = re.compile(r'^#([0-9]+): @ ([0-9]+) / ([0-9]+) .*$')
PROGNAME = "extractor.py"
VERSION = "3.1"
AUTHOR = "Giuseppe Insana"
args = None


def unzip(iterable):
    """
    reverse of zip
    """
    return zip(*iterable)


def batch(iterable, batchsize=1):
    """
    split iterable in constant-size chunks
    """
    length = len(iterable)
    for index in range(0, length, batchsize):
        yield iterable[index:min(index + batchsize, length)]


def find_chunkstart_size(startpos, entrysize):
    """
    use u2c_map to calculate whence to dl from
    and use compressratio and entrysize
    to estimate how much needed to dl
    if cache used, check if file already exists otherwise setup to dl whole chunk
    """
    gzchunk_file = None #only used if keepcache
    #print_u2c_map() #debug
    nearest_index = index_binary_search(1, args.u2c_maxindex, startpos)
    if nearest_index is None:
        eprint("    => ERROR: Could not extract correct uncompressed/compressed byte mapping")
        sys.exit(5)
    uncompressed_chunkstart = args.u2c_index[nearest_index]
    chunkstart = max(0, args.u2c_map[uncompressed_chunkstart] - 1) #1 byte before index point start
    if args.keepcache:
        gzchunk_file = args.gzchunks_fprefix + "." + str(nearest_index)
        # check if gzchunk already exists in cache
        if os.path.isfile(gzchunk_file) and entrysize <= args.maxentrysize:
            chunksize = 0 #do not download anything, use previously downloaded cached file
        else: # if it doesn't, download a whole gzchunk and store it in cache
            if entrysize > args.maxentrysize:
                if args.verbose:
                    eprint("    => NOTICE: entry size {}, consider increasing MAXENTRYSIZE".format(
                        entrysize))
                args.maxentrysize = entrysize #push up the boundary since we encountered bigger one
            chunksize = GZCHUNKSPAN * args.compress_ratio // OVERDLFACTOR + args.maxentrysize
    else:
        chunksize = (startpos - uncompressed_chunkstart) \
            * args.compress_ratio // OVERDLFACTOR + entrysize
        #NOTE: we add the whole entrysize without applying compress_ratio (avg) as we assume
        #the worst, i.e. we assume worst compression for each single entry

    #if args.verbose: #debug
    #   eprint(" |-- found nearest lower index is {}: {} ---> chunkstart {} ; chunksize {}".format(
    #       nearest_index, uncompressed_chunkstart, chunkstart, chunksize)) #debug
    return chunkstart, chunksize, gzchunk_file


def retrieve_compressed_chunk(chunkstart, chunksize, temp_dl_file, gzchunk_file):
    """
    create a temporary file downloading a chunk of a remote compressed flatfile
    optionally store in cache a whole gzchunk
    """
    #create temp_dl_file containing only chunkstart zeros (not taking space on disk):
    dd_call = ["dd", 'if=/dev/zero', "of={}".format(temp_dl_file),
               "seek={}".format(chunkstart), 'bs=1', 'count=0']
    #eprint(" |-- calling dd as '{}'".format(" ".join(dd_call)))
    #result = run(dd_call, check=True, capture_output=True) #subprocess.run #py3.7only
    result = run(dd_call, check=True, stdout=PIPE, stderr=PIPE) #subprocess.run #py3.6ok
    if result.returncode != 0:
        eprint("    => ERROR: problems with dd command used for compressed temporary file")
        eprint("    => {}".format(result.stderr.decode()))
        sys.exit(1)
    CHUNKFH = open("{}".format(temp_dl_file), 'ab') #append, binary
    #append downloaded chunk to temp_dl_file
    if args.keepcache and args.verbose:
        eprint(" |-- downloading {} bytes for gzchunk cache".format(chunksize)) #debug
    CHUNKFH.write(retrieve_from_size(args.flatfile, chunkstart, chunksize))
    CHUNKFH.close()
    if args.keepcache: #store the downloaded gzchunk in the cache
        #eprint(" |-- storing new gzchunk file in cache: '{}'".format(gzchunk_file)) #debug
        os.rename(temp_dl_file, gzchunk_file)


def retrieve_from_size(url, begin, size):
    """
    reads from remote url a specified amount of bytes
    """
    end = begin + size - 1
    headers = {'user-agent': 'ffdb/2.3.6',
               'Accept-Encoding': 'identity',
               'range': 'bytes={}-{}'.format(begin, end)}

    if url[0:3] == "ftp":
        eprint("    => ERROR: ftp scheme not supported for range retrieval")
        sys.exit(5)
    else: #http
        r = get(url, headers=headers)
    #eprint("request headers: {}".format(r.request.headers)) #debug
    #eprint("response headers: {}".format(r.headers)) #debug
    if r.status_code in (200, 206):
        return r.content
    eprint("    => ERROR: problems retrieving entry, http status code is '{}' ({})".format(
        r.status_code, r.reason))
    return None


def find_compressratio():
    """
    find how much data was compressed
    """
    gztool_call = [GZTOOL_EXE, '-l', args.gzindex]
    result = run(gztool_call, check=True, stdout=PIPE, stderr=PIPE) #subprocess.run
    if result.returncode != 0:
        eprint("    => ERROR: problem using gztool to read compressed_index {}".format(
            args.gzindex))
        eprint("    => {}".format(result.stderr.decode()))
        sys.exit(5)
    compressratio_match = RECOMPRESSRATIO.search(result.stdout.decode())
    if compressratio_match:
        reduced_ratio = compressratio_match.group(1)
        args.compress_ratio = int(10000 - float(reduced_ratio) * 100)
        if args.verbose:
            eprint(" |-- the flatfile is compressed to {}% of the original".format(
                round(100 - float(reduced_ratio), 2)))
    else:
        eprint("    => ERROR: problem using gztool to find compress_ratio")
        eprint("    => {}".format(result.stdout.decode()))
        eprint("    => WARNING: could not deduce correct compressratio from index file,")
        eprint("                for safety we'll download more than strictly necessary")
        args.compress_ratio = 10000


def print_u2c_map():
    """
    print uncompressed to compressed index mapping (debug)
    """
    eprint(" |-- map's max index is {}".format(args.u2c_maxindex))
    for index_point in args.u2c_index:
        eprint(" |-- {}: {} ---> {}".format(index_point, args.u2c_index[index_point],
                                            args.u2c_map[args.u2c_index[index_point]]))


def build_u2c_map():
    """
    to build uncompressed to compressed index mapping
    """
    u2c_index = dict()
    u2c_map = dict()
    gztool_call = [GZTOOL_EXE, '-ll', args.gzindex]
    if args.verbose:
        eprint(" |-- building gztool index: '{}'".format(" ".join(gztool_call)))
    gztool_pipe = Popen(gztool_call, stdout=PIPE,
                        bufsize=1, universal_newlines=True, stderr=DEVNULL)
    for line in gztool_pipe.stdout:
        for index_string in line.split(", "):
            gzindex_match = REGZINDEX.match(index_string)
            if gzindex_match:
                index_point, compressed_byte, uncompressed_byte = gzindex_match.groups()
                u2c_index[int(index_point)] = int(uncompressed_byte)
                u2c_map[int(uncompressed_byte)] = int(compressed_byte)

    args.u2c_maxindex = len(u2c_index)
    args.u2c_index = u2c_index
    args.u2c_map = u2c_map


def check_args():
    """
    parse arguments and check for error conditions
    """
    global args, GZTOOL_EXE
    usagetxt = """{0} -f FLATFILE -i INDEXFILE -s IDENTIFIER
                          or
       {0} -f FLATFILE -i INDEXFILE -l LISTFILE
    [-s] : one or more identifiers (space separated)
    [-l] : filename containing a list of identifiers
    see '{0} -h' for tweaks and optional modes
    \nnotes: FLATFILE can be local or remote (use URL as FLATFILE), it can contain
         encrypted or compressed entries, it can even be gzipped as a whole.
         All types will be handled automatically.
       For gzipped FLATFILE, the gztool exe and a GZINDEX need to be available
         see {0} -h for tweaks and optional modes\nexamples:
    {0} -f entries.dat -i entries.pos -s Q9GJU7 Q8HYU5 >twoentries.dat
      (extract the entries identified by Q9GJU7 and Q8HYU5)
    {0} -f entries.dat -i entries.pos -d -s 9606 >homo.dat
      (extract all entries corresponding to identifier 9606)

    {0} -f entries.dat -i entries.pos -l chosen.ids >chosen.dat
    {0} -f entries.dat.gz -i entries.pos -l chosen.ids -o chosen.dat

    {0} -f http://hostname/db -i db.pos -s duffyduck
    {0} -f http://hostname/db.gz -i db.pos -I db.gzi -s duffyduck
      (from a remote db extract entry 'duffyduck': -I needed if gzipped)
    """.format(PROGNAME)
    parser = argparse.ArgumentParser(description='Use a positional index to retrieve \
                                     entries from a flatfile', usage=usagetxt)
    parser.add_argument('-f', '--file', dest='flatfile',
                        help="filename of flatfile to be processed; can be an URL",
                        required=True, type=str)
    parser.add_argument('-i', '--index', dest='index_filename',
                        help="filename of index file with entry identifiers",
                        required=True, type=str)
    parser.add_argument('-s', '--single', dest='identifiers',
                        help="identifier(s) for the desired entry to be extracted",
                        required=False, type=str, nargs='+')
    parser.add_argument('-t', '--threads', dest='threads',
                        help="use specified number of multiple threads for parallel retrieval",
                        required=False, type=int)
    parser.add_argument('-l', '--list', dest='list_filename',
                        help="a file containing a list of identifiers for entries to retrieve",
                        required=False, type=str)
    parser.add_argument('-o', '--outfile', dest='output_filename',
                        help="optionally write output to file rather than to stdout",
                        required=False, type=str)
    parser.add_argument('-m', '--mergedretrieval', dest='merged', action='store_true',
                        help="merge and retrieve together adjacent entries (requires more memory \
                        and processing but could prove much faster for remote extraction)",
                        required=False)
    parser.add_argument('-v', '--verbose', dest='verbose', action='store_true',
                        help="verbose operation", required=False)
    parser.add_argument('-d', '--duplicates', dest='duplicates', action='store_true',
                        help="specify INDEX_FILE could contain duplicate identifiers and request \
                        extraction of all of them (default is to extract a single entry)",
                        required=False)
    parser.add_argument('-z', '--zfound', dest='zfound', action='store_true',
                        help="specify INDEX_FILE contains duplicate identifiers and request \
                        extraction of last entry appearing in the flatfile (default is the first)",
                        required=False)
    parser.add_argument('-x', '--xsanity', dest='xsanity', action='store_true',
                        help="check checksums (if provided in index) to confirm sanity of \
                        the extracted data (slower extraction)", required=False)
    parser.add_argument('-p', '--passphrase', dest='passphrase',
                        help="specify passphrase to decrypt entries from encrypted flatfile; \
                        not recommended on multiuser systems due to security concerns. By default \
                        the passphrase will be requested interactively", required=False, type=str)
    parser.add_argument('-b', '--blocksize', dest='list_blocksize',
                        help="redefine blocksize used for parallel execution. By default \
                        it will be adjusted automatically to the number of threads",
                        required=False, type=siprefix2num)
    parser.add_argument('-c', '--compressed', dest='compressed', action='store_true',
                        help="specify flatfile is gzipped; a .gzi GZINDEX file is required",
                        required=False)
    parser.add_argument('-g', '--gzindex', dest='gzindex',
                        help="filename of the compressed index .gzi",
                        required=False, type=str)
    parser.add_argument('-r', '--remote', dest='remote', action='store_true',
                        help="specify flatfile is remote and treat FLATFILE as an URL",
                        required=False)
    parser.add_argument('-k', '--keepcache', dest='keepcache', action='store_true',
                        help="keep local cache to consume less bandwidth; only applicable to \
                        remote gzipped flatfiles", required=False)
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
            eprint(" |-- in case of duplicate ids, the last entry in ff will be extracted")
        elif args.duplicates:
            eprint(" |-- in case of duplicate ids, all entries in ff will be extracted")
            if args.threads is not None and not args.merged:
                eprint("    => NOTICE: -m recommended if many entries with same identifier requested in multithreading")

    if args.flatfile[-3:] == ".gz" and not args.compressed:
        eprint("    => NOTICE: -f argument has extension .gz: assuming flatfile is compressed")
        args.compressed = True

    if args.flatfile.find("://") != -1 and not args.remote:
        eprint("    => NOTICE: -f argument appears to be an URL: assuming flatfile is remote")
        args.remote = True

    if not args.remote:
        args.flatfilesize = os.path.getsize(args.flatfile) #find and store local flatfile filesize

    if args.identifiers is None and args.list_filename is None:
        eprint("    => ERROR: At least one of --list or --single has to be specified!")
        sys.exit(22)

    if args.identifiers is not None and args.list_filename is not None:
        eprint("    => ERROR: No sense specifying both --list and --single at the same time")
        sys.exit(22)

    if args.list_blocksize is not None:
        if args.threads is None:
            eprint("    => ERROR: specifying blocksize makes sense only for -t execution")
            sys.exit(22)
        if args.merged:
            eprint("    => NOTICE: ignoring blocksize in mergedretrieval mode")
        else:
            if args.verbose:
                eprint(" |-- blocksize set to {} bytes".format(args.list_blocksize))
    if args.threads is not None: #multithread
        if args.identifiers is not None and not args.merged:
            eprint("    => ERROR: No sense specifying multithreading for --single extraction unless --mergedretrieval is used")
            sys.exit(22)
        if args.threads < 2:
            eprint("    => ERROR: No sense specifying a number of threads lower than 2!")
            sys.exit(22)
        args.mt_subfiles_prefix = TEMPDIR + "/tmpEXTRACTmts" + randnum
        if args.merged:
            args.chunks_count = args.threads
        else:
            if args.list_blocksize is None:
                #if not specified, we use 1/threadnumTH of filesize up to a minimum MINBLOCKSIZE
                args.list_blocksize = max(
                    calculate_blocksize(args.list_filename, args.threads),
                    siprefix2num(MINBLOCKSIZE))
            args.list_filesize, args.chunks_count = calculate_chunknum(
                args.list_filename, args.list_blocksize)
            if args.verbose:
                eprint(" |-- parallel work in {} chunks of maximum {} bytes (-b to change)".format(
                    args.chunks_count, args.list_blocksize))
        if args.verbose:
            eprint(" |-- using maximum {} parallel threads (-t); your OS reports {} cpus.".format(
                args.threads, os.cpu_count()))
    else: # if unspecified, set args.threads to 1
        args.threads = 1
        args.chunks_count = 1

    if args.verbose and args.merged:
        eprint(" |-- using merged retrieval for adjacent entries")

    args.decrypt = False
    args.deflated = False

    #gather information from first line of index
    with open(args.index_filename, 'r', 1) as indexfh:
        index_type, args.cipher_name, args.keysize, has_checksum = check_index(
            indexfh.readline()) #pass first line of index as argument
    if args.xsanity and not has_checksum:
        eprint("    => ERROR: sanity check requested (-x) but index does not contain checksums!")
        sys.exit(1)
    if args.xsanity and args.verbose:
        eprint(" |-- entry checksums will be verified")
    if index_type in (".", "+"):
        args.decrypt = True
        if args.verbose:
            eprint(" |-- index made for encrypted entries")
    if index_type in (":", "+"):
        args.deflated = True
        if args.verbose:
            eprint(" |-- index made for compressed entries")

    if args.decrypt:
        if args.passphrase is None:
            eprint(" |-- entries are encrypted, please provide a passphrase:")
            args.passphrase = getpass.getpass(prompt=" |>> Passphrase: ")
        my_cipher_name, args.key = derive_key(args.passphrase, args.keysize)
        if my_cipher_name != args.cipher_name:
            eprint("    => ERROR: problems with cipher name (this should not have happened!)")
            sys.exit(14)
        if args.verbose:
            eprint(" |-- encrypted flatfile (with cipher {}) will be processed".format(
                args.cipher_name))
            #eprint(" |-- the passphrase is: {}".format(args.passphrase)) #DEBUG!
            #eprint(" |-- the decryption key is: {}".format(args.key)) #DEBUG!
    else: #not encrypted
        if args.passphrase is not None:
            eprint("    => NOTICE: ignoring specified passphrase:")
            eprint("       index was not made for encrypted flatfile!")

    if args.compressed:
        if args.decrypt:
            eprint("    => ERROR: compressed flatfile and encrypted flatfile")
            eprint("       are mutually exclusive.")
            eprint("       If flatfile was compressed during encryption")
            eprint("       this will be handled automatically")
            sys.exit(22)
        if GZTOOL_EXE is None:
            result = run(["which", "gztool"], check=True, stdout=PIPE, stderr=PIPE) #subprocess.run
            if result.returncode != 0:
                eprint("    => ERROR: gztool command not found in your path.")
                eprint("Extraction of entries from compressed file depends on it.")
                eprint("Please install gztool to continue: https://github.com/circulosmeos/gztool")
                eprint("    => {}".format(result.stderr.decode()))
                sys.exit(2)
            GZTOOL_EXE = result.stdout.decode().rstrip()
        if args.gzindex is None:
            if args.remote:
                eprint("    => ERROR: a local compressed index needs to be specified (-g)!")
                sys.exit(22)
            else:
                args.gzindex = args.flatfile + "i" #default when not specified
                if args.verbose:
                    eprint("    => NOTICE: assuming COMPRESSEDINDEX is '{}'.".format(args.gzindex))
                    eprint("       Use -I option to override")
            if args.verbose:
                eprint(" |-- building hash of compressed index points..")
        if args.remote:
            args.dlfiles_prefix = TEMPDIR + "/tmpEXTRACTdl" + randnum

    if args.keepcache:
        if not args.remote or not args.compressed:
            eprint("    => ERROR: --keepcache only applicable for remote compressed flatfiles")
            sys.exit(22)
        args.maxentrysize = MAXENTRYSIZE #boundary can be increased if a bigger entry is requested
        args.remote_name = args.flatfile.split("/")[-1]
        args.gzcache_dir = TEMPDIR + "/tmpEXTRACTcache/" + args.remote_name + "/"
        args.gzchunks_fprefix = args.gzcache_dir + "GZ"
        if args.verbose:
            eprint(" |-- using cache of compressed chunks at '{}'".format(args.gzcache_dir))
            eprint(" |-- (you can use environment variable TMPDIR to change this location)")


def check_files():
    """
    do some checks on availability of resources
    """
    if args.remote:
        try:
            retrieve_from_size(args.flatfile, 0, 16)
        except FileNotFoundError:
            eprint("    => ERROR: Cannot access remote url '{}'".format(args.flatfile))
            sys.exit(2)
    else:
        check_iofiles([args.flatfile], [])
    if args.compressed:
        check_iofiles([args.gzindex], [])
        find_compressratio() #find how much data was compressed
        build_u2c_map() #to build uncompressed to compressed index mapping
    if args.list_filename is not None:
        check_iofiles([args.list_filename], [])
    if args.output_filename is not None:
        check_iofiles([], [args.output_filename])

    if args.threads > 1: #multithread
        args.chunk_tempfh = list()
        args.chunk_tempfiles = list()
        for chunknum in range(args.chunks_count):
            chunknumstr = str(chunknum).zfill(len(str(args.chunks_count))) #e.g. 00, 01..
            chunk_tempfile = args.mt_subfiles_prefix + "." + chunknumstr
            try:
                myoutputfh = open(chunk_tempfile, 'wb')
            except PermissionError:
                eprint("    => ERROR: Cannot open temporary file '{}' for writing".format(
                    chunk_tempfile))
                sys.exit(1)
            args.chunk_tempfh.append(myoutputfh) #store temp filehandles
            args.chunk_tempfiles.append(chunk_tempfile) #store temp filenames

    if args.remote and args.compressed:
        args.chunk_dl_tempfiles = list()
        for chunknum in range(args.chunks_count): #if singlethread, only one
            chunknumstr = str(chunknum).zfill(len(str(args.chunks_count))) #e.g. 00, 01..
            chunk_tempfile = args.dlfiles_prefix + "." + chunknumstr
            try:
                myoutputfh = open(chunk_tempfile, 'wb')
            except PermissionError:
                eprint("    => ERROR: Cannot open temporary file '{}' for writing".format(
                    chunk_tempfile))
                sys.exit(1)
            args.chunk_dl_tempfiles.append(chunk_tempfile)
        if args.keepcache:
            if not os.path.isdir(args.gzcache_dir): #create dir if not already present
                os.makedirs(args.gzcache_dir, mode=0o700, exist_ok=False)
            chunk_tempfile = args.gzchunks_fprefix + ".0"
            try:
                myoutputfh = open(chunk_tempfile, 'wb')
            except PermissionError:
                eprint("    => ERROR: Cannot open temporary cache file '{}' for writing".format(
                    chunk_tempfile))
                sys.exit(1)


def print_entry(entry, identifier, fh, chunknum, merged=None):
    """
    apply eventual post-processing and then print entry to filehandle
    """
    if merged is not None:
        #eprint("got merged: {} of size {}".format(merged, len(entry['content']))) #debug
        for collated in merged:
            subentry = dict()
            #unpack collated information
            if args.xsanity:
                if args.decrypt:
                    position, entry_length, subentry['iv'], \
                        subentry['checksum'], identifier = collated
                else:
                    position, entry_length, subentry['checksum'], identifier = collated
            elif args.decrypt:
                position, entry_length, subentry['iv'], identifier = collated
                #eprint("got subentry {}: {}-{} {}".format(identifier,
                #                                         position, entry_length,
                #                                         subentry['iv'])) #debug
            else:
                position, entry_length, identifier = collated
                #eprint("got subentry {}: {}-{}".format(identifier, position, entry_length)) #debug
            subentry['content'] = entry['content'][position:position + entry_length]
            #now call again print_entry for each entry making up the merged entry
            print_entry(subentry, identifier, fh, chunknum)
    else:
        entryenc = None
        #eprint("printing entry for {} of size {}".format(identifier, len(entry['content']))) #debug
        if args.decrypt:
            iv = b''.fromhex(entry['iv']) #convert from hex to bytes
            cipher = init_cipher(args.key, iv)
            if args.deflated:
                try:
                    entryenc = inflate(cipher.decrypt(entry['content']))
                except IOError:
                    entryenc = None
                    corrupted_count[chunknum] += 1
                    if args.verbose:
                        eprint("    => WARNING: could not decrypt entry for identifier {}".format(
                            identifier))
            else:
                try:
                    entryenc = cipher.decrypt(entry['content'])
                except IOError:
                    entryenc = None
                    corrupted_count[chunknum] += 1
                    if args.verbose:
                        eprint("    => WARNING: could not decrypt or inflate entry")
                        eprint("       for identifier {}".format(identifier))
        elif args.deflated:
            try:
                entryenc = inflate(entry['content'])
            except IOError:
                entryenc = None
                corrupted_count[chunknum] += 1
                if args.verbose:
                    eprint("    => WARNING: could not inflate entry")
                    eprint("       for identifier {}".format(identifier))
        else:
            entryenc = entry['content']

        if entryenc:
            if args.xsanity:
                entry_checksum = int_to_b64(zlib.crc32(entryenc))
                if entry_checksum != entry['checksum']:
                    if args.verbose:
                        eprint("    => WARNING: Skipping entry due to failed checksum sanity check")
                        eprint("                for identifier {}".format(identifier))
                    corrupted_count[chunknum] += 1
                    return
            extracted_count[chunknum] += 1
            if fh.mode == 'w':
                fh.write(entryenc.decode('UTF-8'))
            else:
                fh.write(entryenc)
            fh.flush()
        else:
            if args.verbose:
                eprint("    => WARNING: skipping empty entry for '{}'".format(identifier))
    return


def index_binary_search(min_index, max_index, search_value):
    """
    perform a binary search inside the gzindex of compressed/uncompressed points
    """
    u2c_index = args.u2c_index
    found_index = None
    #eprint("searching for {}".format(search_value)) #debug
    while min_index <= max_index:
        mid_index = (min_index + max_index + 1) // 2
        #eprint("      min: {} max: {} found: {} mid: {}".format(
        #  min_index, max_index, found_index, mid_index))
        if u2c_index[mid_index] <= search_value:
            min_index = mid_index + 1
            found_index = mid_index
        else:
            max_index = mid_index - 1
    return found_index


def fetch_compressed_entry(flatfile, position, entry_length):
    """
    use gztool to extract a compressed_entry
    from given position of specified flatfile
    """
    gztool_call = [GZTOOL_EXE, '-W', '-b', str(position), '-I', args.gzindex, flatfile]
    #eprint(" |-- calling gztool as '{}'".format(" ".join(gztool_call))) #debug

    with Popen(gztool_call, stdout=PIPE, stderr=DEVNULL) as gztool_pipe:
        return gztool_pipe.stdout.read(entry_length)


def collect_index(indexfh, identifier):
    """
    find and decode position of entries corresponding to specified identifier from index
    return zipped arrays of decoded positions, sizes and optionally iv and checksums
    """
    if args.duplicates:
        if args.xsanity:
            positions, checksums = get_positions_checksums(indexfh, identifier)
        else:
            positions = get_positions(indexfh, identifier)
    elif args.zfound:
        if args.xsanity:
            position, checksum = get_position_checksum_last(indexfh, identifier)
            positions = [position]
            checksums = [checksum]
        else:
            positions = [get_position_last(indexfh, identifier)]
    else:
        if args.xsanity:
            position, checksum = get_position_checksum_first(indexfh, identifier)
            positions = [position]
            checksums = [checksum]
        else:
            positions = [get_position_first(indexfh, identifier)]

    if not positions or positions == [None]: #empty array
        if args.verbose:
            eprint("    => WARNING: '{}' not found in index; skipping".format(identifier))
        return []

    found_positions = list()
    found_lengths = list()
    if args.decrypt:
        found_iv = list() #to store iv

    found_count[0] += len(positions)

    identifiers = [identifier] * len(positions) #to identify entries failing to extract

    for position in positions:
        iv = ''
        checksum = ''
        #1) extract position and entry size
        if args.decrypt: #with iv
            posmatch = REESIV.match(position)
            position, entry_length, iv = posmatch.groups()
            found_iv.append(iv)
        else: #no iv
            posmatch = REES.match(position)
            position, entry_length = posmatch.groups()

        found_positions.append(b64_to_int(position)) #integer position
        found_lengths.append(b64_to_int(entry_length)) #integer entry size

    if args.xsanity:
        if args.decrypt:
            return zip(found_positions, found_lengths, found_iv, checksums, identifiers)
        else:
            return zip(found_positions, found_lengths, checksums, identifiers)
    elif args.decrypt:
        return zip(found_positions, found_lengths, found_iv, identifiers)
    else:
        return zip(found_positions, found_lengths, identifiers)


def merge_adjacent(found_indexes):
    """
    find if adjacent entries requested and group them together before extraction
    strategy:
    check whether next entry follows previous.
     if yes: add to a merged entry with same position, sum of entry_sizes,
     collation of remaining data and keeping original entries' size and pos for
     later unpacking by print_entry
     otherwise: keep as normal (isolated) entry
    """
    merged_indexes = list()
    prev_position = prev_length = 0.1 #bogus for first comparison
    prev_rest = []
    merged_length = 0
    merged_position = None
    merged_collated = list()
    for position, entry_length, *rest in found_indexes:
        if prev_position + prev_length == position:
            #eprint('{}-{} is adjacent to {}-{}, MERGING'.format(position, entry_length,
            #                                                   prev_position, prev_length))
            merged_length += prev_length
            if merged_position is None:
                merged_position = prev_position
            merged_collated.append((prev_position - merged_position, prev_length, *prev_rest))
        else:
            #eprint('{}-{} NOT adjacent to {}-{}'.format(position, entry_length,
            #                                           prev_position, prev_length))
            if merged_length:
                merged_length += prev_length
                merged_collated.append((prev_position - merged_position, prev_length, *prev_rest))
                merged_indexes.append((merged_position, merged_length, merged_collated))
                merged_length = 0 #reset
                merged_position = None #reset
                merged_collated = []
            else:
                merged_indexes.append((prev_position, prev_length, *prev_rest))
        prev_position = position
        prev_length = entry_length
        prev_rest = rest
    if merged_length: # if we arrive to the end with an opened merged entry
        #eprint("Final append of merged entry for {}-{}".format(position, entry_length)) #debug
        merged_length += entry_length
        merged_collated.append((position - merged_position, entry_length, *prev_rest))
        merged_indexes.append((merged_position, merged_length, merged_collated))
    else: # last isolated entry
        #eprint("Final append of isolated entry for {}-{}".format(position, entry_length)) #debug
        merged_indexes.append((position, entry_length, *prev_rest))

    merged_indexes.pop(0) #remove bogus first element

    del found_indexes
    return merged_indexes


def extract_entry(flatfilefh, indexfh, identifier, chunknum):
    """
    find position of entry corresponding to specified identifier from index
    calls retrieve_entry (which calls print_entry)
    """
    if args.duplicates:
        if args.xsanity:
            positions, checksums = get_positions_checksums(indexfh, identifier)
        else:
            positions = get_positions(indexfh, identifier)
    elif args.zfound:
        if args.xsanity:
            position, checksum = get_position_checksum_last(indexfh, identifier)
            positions = [position]
            checksums = [checksum]
        else:
            positions = [get_position_last(indexfh, identifier)]
    else:
        if args.xsanity:
            position, checksum = get_position_checksum_first(indexfh, identifier)
            positions = [position]
            checksums = [checksum]
        else:
            positions = [get_position_first(indexfh, identifier)]

    if not positions or positions == [None]: #empty array
        if args.verbose:
            eprint("    => WARNING: '{}' not found in index; skipping".format(identifier))
        return

    found_count[chunknum] += len(positions)
    iv = None
    checksum = None
    for position in positions:
        #1) extract position and entry size
        if args.decrypt: #with iv
            posmatch = REESIV.match(position)
            position, entry_length, iv = posmatch.groups()
        else: #no iv
            posmatch = REES.match(position)
            position, entry_length = posmatch.groups()
        if args.xsanity:
            checksum = checksums.pop(0)

        #2) decode position
        position = b64_to_int(position)
        entry_length = b64_to_int(entry_length)

        #if args.verbose:
        #  eprint(" |-- #{} extracting '{}' from position '{}' length '{}'".format(
        #      chunknum, identifier, position, entry_length)) #debug

        #3) retrieve entry
        retrieve_entry(flatfilefh, identifier, position, entry_length, chunknum, iv=iv,
                       checksum=checksum)


def retrieve_entry(flatfilefh, identifier, position, entry_length, chunknum, iv=None, checksum=None,
                   merged=None):
    """
    Note: for remote flatfiles, flatfilefh is ignored, using instead args.flatfile (the url)
    retrieve the entry from the flatfile
    calls print_entry
    """
    entry = dict()
    entry['iv'] = iv #if given
    entry['checksum'] = checksum #if given
    #eprint("retrieving {}: {}-{}".format(identifier, position, entry_length)) #debug
    if args.compressed: #gzipped flatfile
        if args.remote:
            chunkstart, chunksize, gzchunk_file = find_chunkstart_size(position,
                                                                       entrysize=entry_length)
            if chunksize != 0: #cache file already exists, no need to retrieve
                retrieve_compressed_chunk(chunkstart, chunksize,
                                          args.chunk_dl_tempfiles[chunknum], gzchunk_file)
            #else: #debug
            #   if args.verbose:
            #       eprint(" |-- using cached gzchunk file: '{}'".format(gzchunk_file)) #debug
            if args.keepcache:

                entry['content'] = fetch_compressed_entry(gzchunk_file, position, entry_length)
            else:
                entry['content'] = fetch_compressed_entry(args.chunk_dl_tempfiles[chunknum],
                                                          position, entry_length)
        else:
            entry['content'] = fetch_compressed_entry(args.flatfile,
                                                      position, entry_length)
    else: #not gzipped
        if args.remote:
            entry['content'] = retrieve_from_size(args.flatfile, #url
                                                  position,
                                                  entry_length)
        else:
            entry['content'] = read_from_size(flatfilefh, position, entry_length)
    if args.threads == 1: #final printout
        print_entry(entry, identifier, args.outputfh, chunknum, merged=merged)
    else: #print to separate temp files
        print_entry(entry, identifier, args.chunk_tempfh[chunknum], chunknum, merged=merged)


def collect_extract_entries():
    """
    identify entries to extract and collect them, merge together, then extract them
    """
    global requested_count
    found_indexes = SortedList() #sorted by integer position
    indexfh = open(args.index_filename, 'r', 1)

    if args.list_filename:
        with open(args.list_filename) as listfh:
            for line in listfh:
                found_indexes.update(collect_index(indexfh, line.rstrip()))
                requested_count[0] += 1
    else:
        for identifier in args.identifiers:
            found_indexes.update(collect_index(indexfh, identifier))
            requested_count[0] += 1

    indexfh.close()

    #eprint(" found_indexes: {}".format(list(found_indexes))) #debug

    #merge adjacent entries together for faster extraction
    merged_indexes = merge_adjacent(found_indexes)

    #eprint(" merged_indexes: {}".format(merged_indexes)) #debug

    #use positional information from possibly merged indexes to retrieve and print entries
    if merged_indexes: #if not empty
        if args.verbose:
            found_length = len(found_indexes)
            merged_length = len(merged_indexes)
            if merged_length < found_length:
                eprint(" |-- {} retrievals for {} entries ({} merged)".format(
                    merged_length, found_length, found_length - merged_length))

        if args.threads > 1: #multithread for merged strategy
            #submit indexes to threads in groups
            batchsize = ceil(len(merged_indexes) / args.threads)
            #eprint("working on batches of max {} retrievals".format(batchsize)) #debug
            #eprint("batched merged_indexes: {}".format(list(batch(merged_indexes, batchsize)))) #debug
            #init threads
            pool = Pool(args.threads, initializer=init_thread_post, initargs=(
                extracted_count, corrupted_count))
            if args.progressbar:
                _ = list(tqdm(pool.imap(retrieve_entries, batch(merged_indexes, batchsize)),
                              total=len(merged_indexes), ascii=PROGRESSBARCHARS))
            else:
                pool.imap(retrieve_entries, batch(merged_indexes, batchsize))
            pool.close() #no more work to submit
            pool.join() #wait workers to finish
        else:
            retrieve_entries(merged_indexes)


def retrieve_entries(indexes):
    """
    use the (now merged where possible) indexes to retrieve entries from flatfile
    """
    if args.remote:
        flatfilefh = None
    else:
        flatfilefh = open(args.flatfile, 'rb')

    if current_process().name == 'MainProcess':
        chunknum = 0
    else: #for debug of multithreading
        chunknum = int(format(current_process().name.split('-')[1]))-1 #-1 to start with 0

    #eprint(" [{}] processing: {}".format(chunknum, indexes)) #debug
    #found_positions, found_lengths, found_identifiers = unzip(indexes) #debug
    #eprint(" [{}] found_positions: {}".format(chunknum, found_positions)) #debug
    #eprint(" [{}] found_lengths: {}".format(chunknum, found_lengths)) #debug
    for position, entry_length, *rest in indexes:
        if isinstance(rest[0], list):
            #eprint("{}-{} is merged entry".format(position, entry_length)) #debug
            retrieve_entry(flatfilefh, 'merged', position, entry_length, chunknum, merged=rest[0])
        else:
            #eprint("{}-{} is normal entry: {}".format(position, entry_length, rest[0])) #debug
            if args.xsanity:
                if args.decrypt:
                    retrieve_entry(flatfilefh, rest[0], position, entry_length, chunknum,
                                   iv=rest[1], checksum=rest[2])
                else:
                    retrieve_entry(flatfilefh, rest[0], position, entry_length, chunknum,
                                   checksum=rest[1])
            elif args.decrypt:
                retrieve_entry(flatfilefh, rest[0], position, entry_length, chunknum, iv=rest[1])
            else:
                retrieve_entry(flatfilefh, rest[0], position, entry_length, chunknum)

    if flatfilefh is not None:
        flatfilefh.close()


def extract_entries(chunknum):
    """
    identify entries to extract and extract them
    """
    global requested_count
    indexfh = open(args.index_filename, 'r', 1)
    if args.remote:
        flatfilefh = None
    else:
        flatfilefh = open(args.flatfile, 'rb')

    if args.list_filename:
        with open(args.list_filename) as listfh:
            if args.threads == 1: #single thread
                for line in listfh:
                    extract_entry(flatfilefh, indexfh, line.rstrip(), chunknum)
                    requested_count[chunknum] += 1
            else: #multithread
                for line in chunk_of_lines(listfh, args.list_filesize,
                                           args.list_blocksize, chunknum):
                    extract_entry(flatfilefh, indexfh, line.rstrip(), chunknum)
                    requested_count[chunknum] += 1
    else:
        for identifier in args.identifiers:
            extract_entry(flatfilefh, indexfh, identifier, chunknum)
            requested_count[chunknum] += 1

    indexfh.close()
    if flatfilefh is not None:
        flatfilefh.close()


def print_stats(start_time):
    """
    if verbose print some final statistics on entries extracted (or failed to extraxct)
    """
    found_sum = 0
    requested_sum = 0
    extracted_sum = 0
    corrupted_sum = 0
    #eprint("requested: {} found: {} extracted: {} corrupted: {}".format(list(requested_count),
    #                                                                   list(found_count), #debug
    #                                                                   list(extracted_count),
    #                                                                   list(corrupted_count)))
    for chunknum in range(args.chunks_count):
        requested_sum += requested_count[chunknum]
        found_sum += found_count[chunknum]
        extracted_sum += extracted_count[chunknum]
        corrupted_sum += corrupted_count[chunknum]

    if found_sum == 1:
        eprint(" |-- Found 1 entry in the index.")
    elif found_sum > 0:
        eprint(" |-- Found {} entries in the index.".format(
            found_sum))
    if found_sum < requested_sum:
        if found_sum == 0:
            eprint("    => WARNING: NONE of the {} requested identifiers found in index!".format(
                requested_sum))
        else:
            eprint("    => WARNING: only {} of the {} requested identifiers found in index.".format(
                found_sum, requested_sum))
    if args.xsanity:
        if corrupted_sum > 0:
            eprint("    => WARNING: {} malformed entries were skipped.".format(corrupted_sum))
    if extracted_sum < found_sum:
        if extracted_sum == 0:
            eprint("    => WARNING: NONE of the {} found identifiers extracted".format(found_sum))
            eprint("       from the flatfile.")
        else:
            eprint("    => WARNING: only {} of the {} found identifiers extracted".format(
                extracted_sum, found_sum))
            eprint("       from the flatfile.")
    eprint(" '-- Elapsed: {}, {} entries/sec --'".format(*elapsed_time(start_time, extracted_sum)))


def init_thread(r, f, x, c):
    """
    to initialise multithreaded worker
    """
    global requested_count, found_count, extracted_count, corrupted_count, req_positions
    requested_count = r
    found_count = f
    extracted_count = x
    corrupted_count = c


def init_thread_post(x, c):
    """
    to initialise multithreaded worker after indexes already collected
    for merged_adjacent strategy
    """
    global extracted_count, corrupted_count, req_positions
    extracted_count = x
    corrupted_count = c


if __name__ == '__main__':
    check_args()
    check_files()

    requested_count = Array('i', args.chunks_count)
    extracted_count = Array('i', args.chunks_count)
    corrupted_count = Array('i', args.chunks_count)
    found_count = Array('i', args.chunks_count)

    if args.output_filename is None:
        args.outputfh = sys.stdout
    else:
        args.outputfh = open(args.output_filename, 'wb')
    start_secs = time.time()

    if args.threads > 1: #multithread
        if args.merged:
            collect_extract_entries()
        else:
            #submit chunks to threads
            #init threads
            pool = Pool(args.threads, initializer=init_thread, initargs=(
                requested_count, found_count, extracted_count, corrupted_count))
            if args.progressbar:
                _ = list(tqdm(pool.imap(extract_entries, range(args.chunks_count)),
                              total=args.chunks_count, ascii=PROGRESSBARCHARS))
            else:
                pool.imap(extract_entries, range(args.chunks_count))
            pool.close() #no more work to submit
            pool.join() #wait workers to finish

        #final union of results:
        close_subfiles(args.chunk_tempfh)
        if args.output_filename is None: #then print to stdout
            print_subfiles(args.chunk_tempfiles)
        else:
            print_subfiles(args.chunk_tempfiles, args.outputfh)

        delete_files(args.chunk_tempfiles) #cleanup
    else: #singlethread
        if args.merged:
            collect_extract_entries()
        else:
            extract_entries(0)
    if args.output_filename is not None:
        args.outputfh.close()
    if args.remote and args.compressed:
        delete_files(args.chunk_dl_tempfiles) #cleanup of temp files
    print_stats(start_secs)
