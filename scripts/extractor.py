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
import gzip
from math import ceil
from array import array
from random import randint
from sortedcontainers import SortedList #if sorted output
#OPTIONAL IMPORTS (uncomment any of the following if never needed):
from subprocess import run, Popen, PIPE, DEVNULL #for compressed flatfiles via gztool
from multiprocessing import Pool, Array, current_process, set_start_method #for multithreaded
from tqdm import tqdm #for progress bar
from requests import get #for remote flatfiles via http
#NECESSARY IMPORTS
from ffdb import eprint, inflate, derive_key, check_index, \
    delete_files, delete_file_if_exists, REES, REESIV, GZTOOL_EXE, TEMPDIR, \
    init_cipher, check_iofiles, b64_to_int, \
    calculate_chunknum, calculate_blocksize, chunk_of_lines, siprefix2num, \
    read_from_size, print_subfiles, close_subfiles, elapsed_time, int_to_b64, \
    get_position_first, get_position_last, \
    get_position_checksum_first, get_position_checksum_last, \
    get_positions, get_positions_checksums, PROGRESSBARCHARS

#CUSTOMIZATIONS:
#you can modify the minimum default size of block (chunk) of identifiers list
#to work on for parallel execution
MINBLOCKSIZE = "10k" #10 kb of identifiers list per chunk

# for remote compressed flatfiles we apply a conservative (+40%) estimation
# (from reported average compress_ratio), i.e. we download more than strictly
# necessary, since the compress_ratio is an average and the particular chunk of
# the compressed file could have different compression ratio.
# If many entries fail to be extracted, considering raising the estimation
# (e.g. 5000 for +50%, 4000 for +60%..). Conversely to save bandwidth you may
# lower it (e.g. use 9000 for only +10% overdownload amount)
# [for remote gzipped flatfiles without cache, not used if flatfiles are bgzipped]
OVERDLFACTOR = 6000

# BGZF block size (each span of compressed data):
BGZFBLOCKSIZE = 65280

#CONSTANTS
REGZCOMPRESSRATIO = re.compile(r'^\tCompression factor\s*: ([0-9.]+)%.*$', re.MULTILINE)
REGZUNCOMPRESSSIZE = re.compile(r'^\tSize of uncompressed file: .* \(([0-9]+) Bytes\)$')
REGZINDEX = re.compile(r'^#([0-9]+): @ ([0-9]+) / ([0-9]+) .*$')
PROGNAME = "extractor.py"
VERSION = "5.2"
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


def find_chunkstart_size_common(gzscheme):
    """
    use u2c_map to calculate start and size of compressed blocks to fetch in order to
    extract an entry given its uncompressed position and size
    if cache used, prepare a filename into which caching needed blocks
    for gztool:
        estimate how much needed to dl if cache not used
    """
    def find_chunkstart_size_poly(startpos, entrysize):
        gzchunk_file = None #only used if keepcache
        offset = None #only used by bgzip

        #1) find chunkstart (compressed byte position corresponding to nearest lower uncompressed pos):
        nearest_index_start = gz_u2b(startpos) #get block where uncompressed startpos lies
        chunkstart = gz_block_startpos(nearest_index_start)

        #2) find chunkend (compressed byte position corresponding to nearest lower uncompressed pos):
        endpos = startpos + entrysize
        nearest_index_end = gz_u2b(endpos) #get block where uncompressed endpos lies
        chunkend = gz_block_endpos(nearest_index_end)

        #3) calculate size of compressed chunk to extract:
        chunksize = chunkend - chunkstart
        if gzscheme == "bgzip":
            #4) calculate bytes to skip from uncompressed_chunkstart to arrive to startpos
            offset = startpos - nearest_index_start * BGZFBLOCKSIZE
            #(blockid * BGZFBLOCKSIZE is uncompressed_chunkstart)

        #5) if cache is requested, prepare a name for the chunk
        if args.keepcache:
            gzchunk_file = args.gzchunks_fprefix + "." + str(nearest_index_start) + \
                "-" + str(nearest_index_end)
        else: #otherwise (only applicable to gztool) estimate how much needed to download
            if gzscheme == "gztool":
                uncompressed_chunkstart = args.u2c_index[nearest_index_start]
                chunksize = (startpos - uncompressed_chunkstart) \
                    * args.compress_ratio // OVERDLFACTOR + entrysize
                #NOTE: we add the whole entrysize without applying compress_ratio (avg) as we assume
                #the worst, i.e. we assume worst compression for each single entry

        #if args.verbose: #debug
        #    eprint(" |-- startpos {} (index {}), endpos {} (index {})".format(
        #        startpos, nearest_index_start, endpos, nearest_index_end)) #debug
        #    eprint(" |-- chunkstart {} chunksize {}, offset {}".format(
        #        chunkstart, chunksize, offset)) #debug

        return chunkstart, chunksize, gzchunk_file, offset

    return find_chunkstart_size_poly


def gztool_init_new_zero_file(chunkstart, temp_dl_file):
    """
    create temp_dl_file containing only chunkstart zeros (not taking space on disk)
    """
    dd_call = ["dd", 'if=/dev/zero', "of={}".format(temp_dl_file),
               "seek={}".format(chunkstart), 'bs=1', 'count=0']
    #eprint(" |-- calling dd as '{}'".format(" ".join(dd_call)))
    #result = run(dd_call, check=True, capture_output=True) #subprocess.run #py3.7only
    result = run(dd_call, check=True, stdout=PIPE, stderr=PIPE) #subprocess.run #py3.6ok
    if result.returncode != 0:
        eprint("    => ERROR: problems with dd command used for compressed temporary file")
        eprint("    => {}".format(result.stderr.decode()))
        sys.exit(1)


def gztool_retrieve_and_append_gzchunk(chunkstart, chunksize, gzchunk_file,
                                       filename_final=None,
                                       adjacent_start=None,
                                       gzchunk_adjacent_file=None):
    """
    extend given chunk retrieving remote compressed data and appending to it
    optionally rename it to newname to store in cache
    """
    with open(gzchunk_file, 'ab') as CHUNKFH: #append, binary
        if adjacent_start is not None and os.path.isfile(gzchunk_adjacent_file):
            if args.threads != 1:
                open(gzchunk_adjacent_file+"l", 'a').close() #create lock file
            #eprint(" |-- Avoiding tail download re-using {}".format(gzchunk_adjacent_file)) #debug
            offset = gz_block_startpos(adjacent_start) + 1
            print_subfiles([gzchunk_adjacent_file], fh=CHUNKFH, offset=offset)
            if args.threads != 1:
                delete_file_if_exists(gzchunk_adjacent_file+"l") #remove lock file
        else:
            #eprint(" |-- downloading {} bytes for gzchunk cache".format(chunksize)) #debug
            CHUNKFH.write(retrieve_from_size(args.flatfile, chunkstart, chunksize))
    if filename_final is not None and not os.path.exists(filename_final):
        #eprint(" |-- storing new gzchunk file in cache: '{}'".format(filename_final)) #debug
        os.rename(gzchunk_file, filename_final)
        return filename_final
    return gzchunk_file


def gztool_retrieve_and_prepend_gzchunk(chunkstart, chunksize,
                                        gzchunk_file, offset,
                                        temp_dl_file,
                                        filename_final=None,
                                        adjacent_start=None,
                                        gzchunk_adjacent_file=None):
    """
    extend given chunk retrieving remote compressed data and prepending to it
    optionally rename it to newname to store in cache
    """
    if adjacent_start is not None and os.path.isfile(gzchunk_adjacent_file):
        if args.threads != 1:
            open(gzchunk_adjacent_file+"l", 'a').close() #create lock file
        #eprint(" |-- Avoiding head download re-using {}".format(gzchunk_adjacent_file)) #debug
        with open(temp_dl_file, 'wb') as CHUNKFH: #join together
            print_subfiles([gzchunk_adjacent_file], fh=CHUNKFH)
            print_subfiles([gzchunk_file], CHUNKFH, offset=offset)
        if args.threads != 1:
            delete_file_if_exists(gzchunk_adjacent_file+"l") #remove lock file
    else:
        #eprint(" |-- downloading {} bytes for gzchunk cache".format(chunksize)) #debug
        gztool_init_new_zero_file(chunkstart - 1, temp_dl_file)
        with open(temp_dl_file, 'ab') as CHUNKFH: #append, binary
            CHUNKFH.write(retrieve_from_size(args.flatfile, chunkstart - 1, chunksize + 1))
            print_subfiles([gzchunk_file], CHUNKFH, offset=offset)
    if filename_final is not None and not os.path.exists(filename_final):
        #eprint(" |-- storing new gzchunk file in cache: '{}'".format(filename_final)) #debug
        os.rename(temp_dl_file, filename_final)
        return filename_final
    return gzchunk_file


def gzchunks_merge_adjacent_common(gzscheme):
    """
    merge adjacent gzchunks into new file, removing the two which got merged
    e.g. merging 1-1 with 2-2
    """
    def gzchunks_merge_adjacent_poly(thisstart, thisend, otherstart, otherend):
        gzchunk_file_newname = args.gzchunks_fprefix + "." + str(thisstart) + \
            "-" + str(otherend)
        gzchunk_file = args.gzchunks_fprefix + "." + str(thisstart) + "-" + str(thisend)
        gzchunk_file_other = args.gzchunks_fprefix + "." + str(otherstart) + "-" + str(otherend)
        #append to gzchunk_file the content of gzchunk_file_other
        if gzscheme == "bgzip":
            offset = 0
        else: #gztool
            offset = gz_block_startpos(otherstart) + 1
        with open(gzchunk_file, 'ab') as CHUNKFH:
            print_subfiles([gzchunk_file_other], fh=CHUNKFH, offset=offset)
        delete_files([gzchunk_file_other])
        os.rename(gzchunk_file, gzchunk_file_newname)
    return gzchunks_merge_adjacent_poly


def gzchunks_merge_overlapping_common(gzscheme):
    """
    merge overlapping gzchunks into new file, removing the two which got merged
    e.g. merging 0-3 with 3-4
    """
    def gzchunks_merge_overlapping_poly(thisstart, thisend, otherstart, otherend):
        gzchunk_file_newname = args.gzchunks_fprefix + "." + str(thisstart) + "-" + str(otherend)
        gzchunk_file = args.gzchunks_fprefix + "." + str(thisstart) + "-" + str(thisend)
        gzchunk_file_other = args.gzchunks_fprefix + "." + str(otherstart) + "-" + str(otherend)

        if gzscheme == "bgzip":
            thisendpos = gz_block_endpos(thisend)
            offset = thisendpos - gz_block_startpos(otherstart)
            extended_size = gz_block_endpos(otherend) - thisendpos
            with open(gzchunk_file_other, 'rb') as OTHERFH:
                with open(gzchunk_file, 'ab') as CHUNKFH: #append
                    CHUNKFH.write(read_from_size(OTHERFH, offset, extended_size))
        else: #gztool
            #for next two offset assignments, the first should be +1
            #and the second -1, which cancel each other
            offset = gz_block_startpos(otherstart) #initial zeros of second chunk
            offset += gz_block_endpos(thisend) - gz_block_startpos(otherstart) #overlap portion
            #append to gzchunk_file the content of gzchunk_file_other skipping initial zeros and overlap part
            with open(gzchunk_file, 'ab') as CHUNKFH:
                print_subfiles([gzchunk_file_other], fh=CHUNKFH, offset=offset)
        delete_files([gzchunk_file_other])
        os.rename(gzchunk_file, gzchunk_file_newname)
    return gzchunks_merge_overlapping_poly


def tail_extend_gzchunk_common(gzscheme):
    """
    given a best chosen cached chunk, extend it from the tail
    return filename of chunk to use (gztool)
      OR
    return compressed data (bgzip)
    """
    def tail_extend_gzchunk_poly(gzchunk_file, best_startblock, best_endblock, endblock,
                                 temp_dl_file, bgzf_chunk):

        #eprint(" |-- tail extending {0}-{1} to {0}-{2}".format(best_startblock, best_endblock,
        #                                                       endblock)) #debug
        missing_size = gz_block_endpos(endblock) - gz_block_endpos(best_endblock)

        #if gzchunk-(best_endblock+1)-endblock exists we can avoid dl and use cached data
        gzchunk_adjacent_file = args.gzchunks_fprefix + "." + str(best_endblock + 1) + \
            "-" + str(endblock)

        if gzscheme == "bgzip":
            if os.path.isfile(gzchunk_adjacent_file): #if we already have a file with the data
                #eprint(" |-- Avoiding tail-download re-using {}-{}".format(best_endblock + 1, endblock)) #debug
                with open(gzchunk_adjacent_file, 'rb') as flatfilefh:
                    missing_part = read_from_size(flatfilefh, 0, missing_size)
            else:
                missing_part = retrieve_from_size(args.flatfile,
                                                  gz_block_endpos(best_endblock), #start of tail
                                                  missing_size)
            #combine the compressed data
            bgzf_chunk += missing_part #append
            #we'll now grow the existing cached file with the tail we downloaded

        gzchunk_file_newname = args.gzchunks_fprefix + "." + str(best_startblock) + \
            "-" + str(endblock)

        if args.threads == 1: #no concurrency risks
            if gzscheme == "bgzip":
                with open(gzchunk_file, 'ab') as CHUNKFH: #append, binary
                    CHUNKFH.write(missing_part)
                os.rename(gzchunk_file, gzchunk_file_newname) #new name reflecting its growth
            else: #gztool
                gztool_retrieve_and_append_gzchunk(gz_block_endpos(best_endblock),
                                                   missing_size, gzchunk_file,
                                                   filename_final=gzchunk_file_newname,
                                                   adjacent_start=best_endblock + 1,
                                                   gzchunk_adjacent_file=gzchunk_adjacent_file)
        else: #in case another thread may be reading from this file
            if not os.path.isfile(gzchunk_file_newname): #if not created by another thread meanwhile
                if os.path.isfile(gzchunk_file): #if file to extend has not meanwhile been deleted
                    with open(temp_dl_file, 'wb') as CHUNKFH:
                        open(gzchunk_file+"l", 'a').close() #create lock file
                        print_subfiles([gzchunk_file], fh=CHUNKFH)
                        delete_file_if_exists(gzchunk_file+"l") #remove lock file
                        if gzscheme == "bgzip":
                            CHUNKFH.write(missing_part)
                    if gzscheme == "bgzip":
                        os.rename(temp_dl_file, gzchunk_file_newname)
                    else: #gztool
                        gztool_retrieve_and_append_gzchunk(gz_block_endpos(best_endblock),
                                                           missing_size, temp_dl_file,
                                                           filename_final=gzchunk_file_newname,
                                                           adjacent_start=best_endblock + 1,
                                                           gzchunk_adjacent_file=gzchunk_adjacent_file)
                else:
                    eprint("    => WARNING: cache file we needed disappeared!!")
            if os.path.isfile(gzchunk_file+"l"): #file busy being read by other thread
                open(gzchunk_file+"_", 'a').close() #schedule file for deletion
            else:
                delete_file_if_exists(gzchunk_file) #delete, hopefully without any concurrency happening

        if gzscheme == "bgzip":
            return bgzf_chunk
        else: #gztool
            return gzchunk_file_newname

    return tail_extend_gzchunk_poly


def head_extend_gzchunk_common(gzscheme):
    """
    given a best chosen cached chunk, extend it from the head
    return filename of chunk to use (gztool)
      OR
    return compressed data (bgzip)
    """
    def head_extend_gzchunk_poly(gzchunk_file, best_startblock, best_endblock, startblock,
                                 temp_dl_file, bgzf_chunk):

        #eprint(" |-- head extending {0}-{1} to {2}-{1}".format(best_startblock, best_endblock,
        #                                                       startblock)) #debug
        #if gzchunk-startblock-(best_startblock-1) exists we can avoid dl and use cached data
        gzchunk_adjacent_file = args.gzchunks_fprefix + "." + str(startblock) + \
            "-" + str(best_startblock - 1)
        missing_size = gz_block_startpos(best_startblock) - gz_block_startpos(startblock)

        if gzscheme == "bgzip":
            if os.path.isfile(gzchunk_adjacent_file): #if we already have a file with the data
                #eprint(" |-- Avoiding head-download re-using {}-{}".format(startblock, best_startblock - 1)) #debug
                with open(gzchunk_adjacent_file, 'rb') as flatfilefh:
                    missing_part = read_from_size(flatfilefh, 0, missing_size)
            else:
                missing_part = retrieve_from_size(args.flatfile,
                                                  gz_block_startpos(startblock), #start of head
                                                  missing_size)
            bgzf_chunk = missing_part + bgzf_chunk #prepend
        else: #gztool
            missing_size += 1
            offset = gz_block_startpos(best_startblock) + 1

        #replace existing cached file with a new bigger file prepending downloaded head
        gzchunk_file_newname = args.gzchunks_fprefix + "." + str(startblock) + \
            "-" + str(best_endblock)

        if args.threads == 1:
            if gzscheme == "bgzip":
                with open(temp_dl_file, 'wb') as CHUNKFH:
                    CHUNKFH.write(missing_part)
                    print_subfiles([gzchunk_file], fh=CHUNKFH)
                os.rename(temp_dl_file, gzchunk_file_newname)
            else: #gztool
                gztool_retrieve_and_prepend_gzchunk(gz_block_startpos(startblock), #start of head
                                                    missing_size,
                                                    gzchunk_file,
                                                    offset,
                                                    temp_dl_file,
                                                    filename_final=gzchunk_file_newname,
                                                    adjacent_start=startblock,
                                                    gzchunk_adjacent_file=gzchunk_adjacent_file)
            os.remove(gzchunk_file)
        else:
            if not os.path.isfile(gzchunk_file_newname): #check if not created by another thread meanwhile
                if os.path.isfile(gzchunk_file): #check if file to extend has not meanwhile been deleted
                    open(gzchunk_file+"l", 'a').close() #create lock file
                    if gzscheme == "bgzip":
                        with open(temp_dl_file, 'wb') as CHUNKFH:
                            CHUNKFH.write(missing_part)
                            print_subfiles([gzchunk_file], fh=CHUNKFH)
                        os.rename(temp_dl_file, gzchunk_file_newname)
                    else: #gztool
                        gztool_retrieve_and_prepend_gzchunk(gz_block_startpos(startblock), #start of head
                                                            missing_size,
                                                            gzchunk_file,
                                                            offset,
                                                            temp_dl_file,
                                                            filename_final=gzchunk_file_newname,
                                                            adjacent_start=startblock,
                                                            gzchunk_adjacent_file=gzchunk_adjacent_file)
                    delete_file_if_exists(gzchunk_file+"l") #remove lock file
                else:
                    eprint("    => WARNING: cache file we needed disappeared!!")
            if os.path.isfile(gzchunk_file+"l"): #file busy being read by other thread
                open(gzchunk_file+"_", 'a').close() #schedule file for deletion
            else:
                delete_file_if_exists(gzchunk_file) #delete, hopefully without any concurrency happening

        if gzscheme == "bgzip":
            return bgzf_chunk
        else: #gztool
            return gzchunk_file_newname

    return head_extend_gzchunk_poly


def retrieve_cached_chunk_common(gzscheme):
    """
    check if we already have in cache what we need otherwise
    download only what is necessary and extend existing chunks building up the cache
    return the correct file from which to extract the entry (gztool)
      OR
    return the content to be decompressed on the fly (bgzip)
    """
    def retrieve_cached_chunk_poly(chunkstart, chunksize, temp_dl_file, gzchunk_file):
        startblock, endblock = os.path.basename(gzchunk_file).split('.')[1].split('-') #requested start and end blocks
        startblock = int(startblock)
        endblock = int(endblock)
        blocksnum = endblock - startblock + 1 #requested total number of blocks
        best_startblock, best_endblock, best_blocksnum = find_best_cached_chunk(startblock,
                                                                                endblock,
                                                                                blocksnum)
        bgzf_chunk = None
        #eprint(" |-- best solution uses {}-{}".format(best_startblock, best_endblock)) #debug

        if best_blocksnum < blocksnum: #if found a solution which would entail less dl
            #prepare name of gzchunk we need
            gzchunk_file = args.gzchunks_fprefix + "." + str(best_startblock) + \
                "-" + str(best_endblock)

            if not os.path.isfile(gzchunk_file):
                #if file we need has meanwhile been deleted by another thread, download it new
                eprint(" |-- cache file we needed {}-{} disappeared: reverting to dl new".format(
                    best_startblock, best_endblock)) #debug
                return retrieve_from_size_and_save(args.flatfile,
                                                   chunkstart, chunksize,
                                                   temp_dl_file, filename_final=gzchunk_file)

            #CODE ONLY RELEVANT TO BGZIP:
            if gzscheme == 'bgzip':
                #calculate amount to skip if needed:
                offset = gz_block_startpos(startblock) - gz_block_startpos(best_startblock)
                #read data from best solution
                if args.threads != 1:
                    open(gzchunk_file+"l", 'a').close() #create lock file
                #eprint(" |-- reading from {}-{} with offset {} size {}".format(best_startblock, best_endblock, max(0, offset), chunksize + min(offset, 0))) #debug
                with open(gzchunk_file, 'rb') as CHUNKFH:
                    bgzf_chunk = read_from_size(
                        CHUNKFH,
                        max(0, offset), #amount to skip from start (if pos offset)
                        chunksize + min(offset, 0)) #amount to trim from end (if neg offset)
                #eprint(" |-- got {} bytes".format(len(bgzf_chunk)))
                if args.threads != 1:
                    delete_file_if_exists(gzchunk_file+"l") #remove lock file

            if best_blocksnum == 0: #no need to dl anything, we already got what we need
                if gzscheme == 'bgzip':
                    return bgzf_chunk
                else: #gztool
                    return gzchunk_file
            elif best_endblock < endblock: #we'll need to download the missing tail
                return tail_extend_gzchunk(gzchunk_file, best_startblock, best_endblock, endblock, temp_dl_file, bgzf_chunk)
            else: #i.e. best_startblock > startblock, we'll need to download missing head
                return head_extend_gzchunk(gzchunk_file, best_startblock, best_endblock, startblock, temp_dl_file, bgzf_chunk)
        else: #no good cache available, retrieve wholly from remote url
            #eprint(" |-- fetching {0}-{1}".format(startblock, endblock)) #debug
            return retrieve_from_size_and_save(args.flatfile,
                                               chunkstart, chunksize,
                                               temp_dl_file, filename_final=gzchunk_file)

    return retrieve_cached_chunk_poly


def find_best_cached_chunk(startblock, endblock, blocksnum):
    """
    look among already downloaded chunks whether there is one which
    totally or partially contains the data we need
    """
    best_blocksnum = blocksnum #init with requested num of blocks (max to dl)
    #get list of all cached files
    blockpair_strings = [x.split('.')[1] for x in os.listdir(args.gzcache_dir)
                         if x[-1] != "l"] #skip lock files

    #remove those marked for deletion
    markers_for_deletion = [x for x in blockpair_strings if x[-1] == "_"]
    marked_for_deletion = [x[:-1] for x in markers_for_deletion]
    blockpair_strings = [x for x in blockpair_strings
                         if x[-1] != "_" and x not in marked_for_deletion]

    #sort chunks by closeness to requested chunk (and amount of overlap)
    #first results will be those closest and with maximum overlap
    blockpair_strings.sort(key=lambda e: chunk_dist_from(startblock, endblock, e))
    best_startblock = None
    best_endblock = None

    for bps in blockpair_strings:
        #eprint(" |-- checking chunk {}".format(bps))
        if chunk_dist_from(startblock, endblock, bps) > best_blocksnum:
            #eprint(" |-- we're already beyond max-to-dl distance, ignoring cache")
            break
        #otherwise we continue and look for best solution,
        #either inside, with tail overlap or with head overlap
        cached_startblock, cached_endblock = [int(x) for x in bps.split('-')]
        if cached_startblock <= startblock:
            #checking promising backward chunk
            if cached_endblock >= endblock: #found match containing the whole chunk we need
                #best solution: no need to download anything, no need to look further
                best_startblock = cached_startblock
                best_endblock = cached_endblock
                best_blocksnum = 0
                #eprint(" -- found best solution {}-{} for {}-{}".format(cached_startblock, cached_endblock, startblock, endblock))
                break
            else: #cached_start <= req_start but cached_endblock < req_end
                missing_part = endblock - cached_endblock #number of new blocks to dl using this
                if missing_part < best_blocksnum:
                    #eprint(" -- found partial match {}-{}".format(cached_startblock, cached_endblock))
                    best_blocksnum = missing_part
                    best_startblock = cached_startblock
                    best_endblock = cached_endblock
                    break #as we sorted the list, there should be no better solution
        elif cached_endblock >= endblock: #(but cached_start > req_start)
            #checking promising forward chunk
            missing_part = cached_startblock - startblock
            if missing_part < best_blocksnum:
                #eprint(" -- found partial solution {}-{}".format(cached_startblock, cached_endblock))
                best_blocksnum = missing_part
                best_startblock = cached_startblock
                best_endblock = cached_endblock
                break #as we sorted the list, there should be no better solution
    return best_startblock, best_endblock, best_blocksnum


def retrieve_from_size_and_save_bgzip(url, begin, size, filename, filename_final=None):
    """
    reads from remote url a specified amount of bytes and save them to a file
    optionally move to a new file once download is completed
    """
    content = retrieve_from_size(url, begin, size)
    with open(filename, 'wb') as fh:
        fh.write(content)
    if filename_final is not None:
        if not os.path.isfile(filename_final): #unless destination file exists already
            os.rename(filename, filename_final) #rename
    return content


def retrieve_from_size_and_save_gztool(url, begin, size, filename, filename_final=None):
    """
    reads from remote url a specified amount of bytes and save them to a file
    after first init with zeros the file
    optionally move to a new file once download is completed
    """
    gztool_init_new_zero_file(begin, filename)
    return gztool_retrieve_and_append_gzchunk(begin, size, filename, filename_final=filename_final)


def retrieve_from_size(url, begin, size):
    """
    reads from remote url a specified amount of bytes
    """
    end = begin + size - 1
    headers = {'user-agent': 'ffdb/2.4.4',
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


def get_remote_size(url):
    """
    fetches and returns size of a remote url
    """
    headers = {'user-agent': 'ffdb/2.4.4', 'Accept-Encoding': 'identity'}

    r = get(url, headers=headers, stream=True)
    if r.status_code in (200, 206):
        return r.headers['Content-length']
    eprint("    => ERROR: problems retrieving entry, http status code is '{}' ({})".format(
        r.status_code, r.reason))
    return -1


def chunkdist(this, other):
    """
    given two segments defined as 'STARTPOS-ENDPOS' return their distance
    0 dist means adjacent; negative indicates overlap
    """
    thisstart, thisend = [int(x) for x in this.split('-')]
    otherstart, otherend = [int(x) for x in other.split('-')]
    return max(thisstart, otherstart) - min(thisend, otherend)


def chunk_dist_from(thisstart, thisend, other):
    """
    given first segments defined as STARTPOS, ENDPOS
    and second segment defined as 'STARTPOS-ENDPOS'
    return their distance
    0 dist means adjacent; negative indicates overlap
    used as sorting function in retrieve_cached_chunk_bgzip()
    """
    otherstart, otherend = [int(x) for x in other.split('-')]
    return max(thisstart, otherstart) - min(thisend, otherend)


def cache_cleanup():
    """
    house-cleaning and reorganization of compressed remote files cache
    """
    #phase 0.1: delete residual lock files from botched executions
    gzchunks_to_delete = [x for x in os.listdir(args.gzcache_dir) if x[-1] == "l"]
    if gzchunks_to_delete:
        delete_files(gzchunks_to_delete, path=args.gzcache_dir) #delete the lock files

    #phase 0.2: delete files scheduled for deletion, if any
    gzchunks_to_delete = [x for x in os.listdir(args.gzcache_dir) if x[-1] == "_"]
    if gzchunks_to_delete:
        delete_files(gzchunks_to_delete, path=args.gzcache_dir) #delete the markers
        gzchunks_to_delete = [x[:-1] for x in gzchunks_to_delete]
        delete_files(gzchunks_to_delete, path=args.gzcache_dir) #delete the files
        eprint(" |-- cache cleanup 0: deleted {} redundant files".format(len(gzchunks_to_delete)))

    #build data structure for sorted list of existing chunks
    #blockpair_strings = [x.split('.')[1] for x in os.listdir(args.gzcache_dir)]
    #blockpair_strings.sort(key=lambda e: int(e.split('-')[0])) #sort by starting blockid
    blockpair_strings = SortedList([x.split('.')[1] for x in os.listdir(args.gzcache_dir)],
                                   key=lambda e: int(e.split('-')[0])) #sorted by integer position

    #for bps in blockpair_strings: eprint("gzchunk {}".format(bps)) #debug

    #phase I: remove smaller chunks included in bigger, if any remaining
    #(could also be residue from previous interrupted cleaning)
    #e.g. if we have 1-5 and 2-3, 2-3 must go
    todelete = set()
    for i in range(len(blockpair_strings)):
        this = blockpair_strings[i]
        if this in todelete:
            continue
        gzchunk_file = args.gzchunks_fprefix + "." + this
        if os.path.getsize(gzchunk_file) == 0:
            #eprint("deleting empty cache chunk file {}".format(gzchunk_file)) #debug
            os.remove(gzchunk_file)
            todelete.add(this) #schedule for removal from list
            continue
        thisstart, thisend = [int(x) for x in this.split('-')]
        for j in range(i + 1, len(blockpair_strings)):
            other = blockpair_strings[j]
            otherstart, otherend = [int(x) for x in other.split('-')]
            if otherstart >= thisstart and otherend <= thisend:
                #eprint("{} contains {}: removing the latter".format(this, other)) #debug
                gzchunk_file = args.gzchunks_fprefix + "." + str(otherstart) + "-" + str(otherend)
                delete_files([gzchunk_file])
                todelete.add(other) #schedule for removal from list
    if todelete:
        eprint(" |-- cache cleanup 1: deleted {} redundant smaller files".format(len(todelete)))
        for other in todelete:
            blockpair_strings.remove(other) #remove from list of blocks, since we deleted it

    #phase II: join together adjacent chunks (also check for new phase I)
    #e.g. if we have 1-5 and 6-8 we can combine into 1-8
    todelete = set() #reset
    nomore_to_join = False
    joined_counter = 0
    while nomore_to_join is False:
        for i in range(len(blockpair_strings)):
            this = blockpair_strings[i]
            if this in todelete:
                continue
            thisstart, thisend = [int(x) for x in this.split('-')]
            for j in range(i + 1, len(blockpair_strings)):
                other = blockpair_strings[j]
                if other in todelete:
                    continue
                otherstart, otherend = [int(x) for x in other.split('-')]
                if otherstart >= thisstart and otherend <= thisend:
                    eprint("{} contains {}: removing the latter".format(this, other))
                    gzchunk_file = args.gzchunks_fprefix + "." + str(otherstart) + "-" + str(otherend)
                    delete_files([gzchunk_file])
                    todelete.add(other) #schedule for removal from list
                    continue
                elif otherstart == thisend + 1:
                    #eprint("{} and {} are adjacent: joining them together".format(this, other)) #debug
                    joined_counter += 1
                    gzchunks_merge_adjacent(thisstart, thisend, otherstart, otherend)
                    todelete.add(this)
                    todelete.add(other)
                    blockpair_strings.add(str(thisstart) + "-" + str(otherend)) # a new file has been created
                    continue #restart checking from start as we may join more
            #else: #continue if inner loop not broken
                #continue
            #break #inner loop was broken, break outer loop
        if todelete:
            for other in todelete:
                blockpair_strings.remove(other) #remove from list of blocks, since we deleted it
            todelete.clear() #empty
        else:
            nomore_to_join = True
    if joined_counter:
        eprint(" |-- cache cleanup 2: performed {} join operations".format(joined_counter))

    #phase III: join partially overlapping blocks
    #e.g. if we have 1-5 and 4-7, we can combine into 1-6, appending 6-7 (part of 4-7) to 1-5
    todelete = set() #reset
    nomore_to_join = False
    joined_counter = 0 #reset
    while nomore_to_join is False:
        for i in range(len(blockpair_strings)):
            this = blockpair_strings[i]
            thisstart, thisend = [int(x) for x in this.split('-')]
            for j in range(i + 1, len(blockpair_strings)):
                other = blockpair_strings[j]
                otherstart, otherend = [int(x) for x in other.split('-')]
                if otherstart >= thisstart and otherend <= thisend:
                    eprint("{} contains {}: removing the latter".format(this, other))
                    gzchunk_file = args.gzchunks_fprefix + "." + str(otherstart) + "-" + str(otherend)
                    delete_files([gzchunk_file])
                    todelete.add(other) #schedule for removal from list
                    continue
                elif otherstart <= thisend: #overlap
                    #eprint("{} and {} are overlapping: joining them together".format(this, other)) #debug
                    joined_counter += 1
                    gzchunks_merge_overlapping(thisstart, thisend, otherstart, otherend)
                    todelete.add(this)
                    todelete.add(other)
                    blockpair_strings.add(str(thisstart) + "-" + str(otherend)) # a new file has been created
                    break #restart checking from start as we may combine more
            else: #continue if inner loop not broken
                continue
            break #inner loop was broken, break outer loop
        if todelete:
            for other in todelete:
                blockpair_strings.remove(other) #remove from list of blocks, since we deleted it
            todelete.clear() #empty
        else:
            nomore_to_join = True
    if joined_counter:
        eprint(" |-- cache cleanup 3: performed {} join operations".format(joined_counter))


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
    compressratio_match = REGZCOMPRESSRATIO.search(result.stdout.decode())
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

    if args.remote:
        args.compressedsize = int(get_remote_size(args.flatfile))
        if args.compressedsize == -1:
            eprint("    => ERROR: Cannot access remote url '{}' to find its size".format(args.flatfile))
            sys.exit(2)
    else:
        args.compressedsize = os.path.getsize(args.flatfile)


def print_u2c_map_gztool():
    """
    print uncompressed to compressed index mapping (debug)
    """
    eprint(" |-- map's max index is {}".format(args.u2c_maxindex))
    for index_point in range(0, args.u2c_maxindex + 1):
        eprint(" |-- {}: {} ---> {}".format(index_point, args.u2c_index[index_point],
                                            args.b2c_map[index_point]))


def print_u2c_map_bgzip():
    """
    print uncompressed to compressed index mapping (debug)
    """
    eprint(" |-- map's max index is {}".format(args.u2c_maxindex))
    for index_point in range(0, args.u2c_maxindex + 1):
        eprint(" |-- {}: {} ---> {}".format(index_point, BGZFBLOCKSIZE * index_point,
                                            args.b2c_map[index_point]))


def build_u2c_map_gztool():
    """
    to build uncompressed to compressed index mapping for gztool index
    """
    u2c_index = list()
    b2c_map = list() #blockid to compressed start
    gztool_call = [GZTOOL_EXE, '-ll', args.gzindex]
    if args.verbose:
        eprint(" |-- building gztool index: '{}'".format(" ".join(gztool_call)))
    gztool_pipe = Popen(gztool_call, stdout=PIPE,
                        bufsize=1, universal_newlines=True, stderr=DEVNULL)
    index_point_check = 1 #gztool indexes start from 1
    #args.uncompressedsize = None #not needed
    for line in gztool_pipe.stdout:
        for index_string in line.split(", "):
            gzindex_match = REGZINDEX.match(index_string)
            if gzindex_match:
                index_point, compressed_byte, uncompressed_byte = gzindex_match.groups()
                if index_point_check != int(index_point):
                    eprint("    => ERROR: gzindex appears to be corrupted: {} != {} for {}, redownload?".format(index_point_check, index_point, gzindex_match))
                    sys.exit(80)
                index_point_check += 1
                uncompressed_byte = int(uncompressed_byte)
                u2c_index.append(uncompressed_byte)
                b2c_map.append(int(compressed_byte))
            #elif args.uncompressedsize is None: #not needed
            #   uncompressed_match = REGZUNCOMPRESSSIZE.match(index_string)
            #   if uncompressed_match:
            #       args.uncompressedsize = uncompressed_match.group(1)

    args.u2c_maxindex = len(u2c_index) - 1
    args.u2c_index = u2c_index
    args.b2c_map = b2c_map

    #set name of what file would be present when completely cached
    if args.keepcache:
        args.gzchunks_complete = args.gzchunks_fprefix + ".0-" + str(args.u2c_maxindex)
        if os.path.isfile(args.gzchunks_complete):
            if args.verbose:
                eprint(" |-- remote file completely cached: no more downloads needed")
            args.gzchunks_completed = True
            args.flatfile = args.gzchunks_complete
            args.remote = False #no need to download anything anymore


def build_u2c_map_bgzip():
    """
    to build uncompressed to compressed index mapping for bgzip index
    bgzip .gzi index format:
            uint64_t number_entries
        followed by number_entries pairs of:
            uint64_t compressed_offset
            uint64_t uncompressed_offset
    """
    if args.verbose:
        eprint(" |-- reading bgzip index")
    arr = array('L') #uint64_t
    gzindex_filesize = os.path.getsize(args.gzindex)
    if gzindex_filesize % 8 != 0:
        eprint("    => ERROR: gzindex appears to be corrupted, redownload?")
        sys.exit(80)
    integers_count = gzindex_filesize // 8

    with open(args.gzindex, 'rb') as fileobj:
        arr.fromfile(fileobj, integers_count)
    arr_len = len(arr)

    total_entries = arr.pop(0) #first number is number of entries
    if total_entries != arr_len // 2:
        eprint("    => ERROR: gzindex appears to be incomplete, redownload?")
        sys.exit(80)

    if args.verbose:
        eprint(" |-- bgzip index contains {} index points".format(total_entries))

    b2c_map = list()
    b2c_map.append(0)
    for compressed_byte, uncompressed_byte in (arr[i:i+2] for i in range(0, arr_len - 1, 2)):
        b2c_map.append(compressed_byte)

    args.u2c_maxindex = total_entries
    args.b2c_map = b2c_map
    #args.uncompressedsize = arr[-1] #not correct for very small bgzipped files
    args.compressedsize = b2c_map[-1] + 28 #not correct for very small bgzipped files
    #eprint(" |-- compressed/uncompressed filesizes: {}/{}".format(args.compressedsize, args.uncompressedsize)) #debug
    if args.remote:
        args.compressedsize = int(get_remote_size(args.flatfile))
        if args.compressedsize == -1:
            eprint("    => ERROR: Cannot access remote url '{}' to find its size".format(args.flatfile))
            sys.exit(2)
    else:
        args.compressedsize = os.path.getsize(args.flatfile)
    #set name of what file would be present when completely cached
    if args.keepcache:
        args.gzchunks_complete = args.gzchunks_fprefix + ".0-" + str(args.u2c_maxindex)
        if os.path.isfile(args.gzchunks_complete):
            if args.verbose:
                eprint(" |-- remote file completely cached: no more downloads needed")
            args.gzchunks_completed = True
            args.flatfile = args.gzchunks_complete
            args.remote = False #no need to download anything anymore


def gz_block_startpos_gztool(blockid):
    """
    start position (in compressed bytes) of a given gztool blockid
    """
    if blockid == 0:
        return 0
    return args.b2c_map[blockid] - 1 #1 byte before index point start


def gz_block_startpos_bgzip(blockid):
    """
    start position (in compressed bytes) of a given bgzf blockid
    """
    if blockid == 0:
        return 0
    return args.b2c_map[blockid]


def gz_block_endpos(blockid):
    """
    end position (in compressed bytes) of a given compressed file blockid
    """
    if blockid == args.u2c_maxindex:
        return args.compressedsize
    else:
        return args.b2c_map[blockid + 1]


def u2b_gztool(uncompressed_position, min_index=0):
    """
    return the gztool blockid holding the position corresponding to the uncompressed position given
    """
    if uncompressed_position == 0:
        return 0 #first block
    blockid = index_binary_search(min_index, args.u2c_maxindex, uncompressed_position)
    if blockid is None:
        eprint("    => ERROR: Could not extract correct uncompressed/compressed byte mapping")
        sys.exit(5)
    return blockid


def u2b_bgzip(uncompressed_position):
    """
    return the bgzf blockid holding the position corresponding to the uncompressed position given
    """
    if uncompressed_position == 0:
        return 0 #first block
    #if uncompressed_position >= args.uncompressedsize:
    #   return args.u2c_maxindex #last block
    blockid = uncompressed_position // BGZFBLOCKSIZE
    return blockid


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
    {0} -f http://hostname/db.gz -i db.pos -g db.gz.gzi -s duffyduck
      (from a remote db extract entry 'duffyduck': -g needed if (b)gzipped)
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
                        help="use specified number of multiple threads for parallel retrieval. \
                        See also -b option.", required=False, type=int)
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
                        help="define blocksize (size of LIST_FILENAME chunks) for parallel \
                        extraction. This is fast and has lowest memory requirements, but could \
                        be less efficient if lots of entries are mapped to same identifier when \
                        --duplicates is used. If unspecified, it will be adjusted automatically \
                        to number of threads. Use -b 0 to disable block extraction",
                        required=False, type=siprefix2num)
    parser.add_argument('-c', '--compressed_gzip', dest='compressed_gzip', action='store_true',
                        help="specify flatfile is gzipped; a .gzi GZINDEX file is required",
                        required=False)
    parser.add_argument('-C', '--Compressed_bgzip', dest='compressed_bgzip', action='store_true',
                        help="specify flatfile is bgzipped; a .gzi GZINDEX file is required \
                        if flatfile is remote",
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
            if not args.merged and (args.list_blocksize is None or args.list_blocksize != 0):
                eprint("    => NOTICE: if many entries with same identifier requested consider using -b 0 or -m")

    args.compressed = False
    if args.compressed_bgzip or args.compressed_gzip: #if specified
        args.compressed = True

    if args.flatfile[-4:] == ".bgz" and not args.compressed:
        eprint("    => NOTICE: extension .bgz: assuming flatfile compressed with bgzip, use -c if gzip compressed")
        args.compressed_bgzip = True
        args.compressed = True
    elif args.flatfile[-3:] == ".gz" and not args.compressed:
        eprint("    => NOTICE: extension .gz: assuming flatfile compressed with gzip, use -C if bgzip compressed")
        args.compressed_gzip = True
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
        if args.identifiers is not None and args.list_blocksize != 0:
            #(if set to 0 it means blockextraction is disabled)
            eprint("    => ERROR: No sense specifying a blocksize with --single identifiers")
            sys.exit(22)

    if args.merged:
        if args.list_blocksize is not None:
            eprint("    => NOTICE: --blocksize cannot be used with --mergedretrieval, ignoring -b")
        args.list_blocksize = 0 #disable

    if args.threads is not None: #multithread
        if args.threads < 2:
            eprint("    => ERROR: No sense specifying a number of threads lower than 2!")
            sys.exit(22)
        args.mt_subfiles_prefix = TEMPDIR + "/tmpEXTRACTmts" + randnum
        if args.list_filename:
            if args.list_blocksize is None: #if not specified, we compute it
                args.list_blocksize = max(calculate_blocksize(args.list_filename, args.threads),
                                          siprefix2num(MINBLOCKSIZE))
        else: #single identifiers
            args.list_blocksize = 0

        if args.list_blocksize != 0: #if not disabled or incompatible
            #we'll use blockextraction without prior collection
            args.list_filesize, args.chunks_count = calculate_chunknum(
                args.list_filename, args.list_blocksize)
            if args.verbose:
                eprint(" |-- parallel work on {} chunks of maximum {} bytes (-b to change)".format(
                    args.chunks_count, args.list_blocksize))
        else: #either mergedretrieval or parallel without blocksize (we'll first collect indexes)
            args.chunks_count = args.threads
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
                args.gzindex = args.flatfile + ".gzi" #default when not specified
                if args.verbose:
                    eprint("    => NOTICE: assuming COMPRESSEDINDEX is '{}'.".format(args.gzindex))
                    eprint("       Use -g option to override")
        if args.remote:
            args.dlfiles_prefix = TEMPDIR + "/tmpEXTRACTdl" + randnum

    if args.keepcache:
        if not args.remote or not args.compressed:
            eprint("    => ERROR: --keepcache only applicable for remote compressed flatfiles")
            sys.exit(22)
        args.remote_name = args.flatfile.split("/")[-1]
        args.gzcache_dir = TEMPDIR + "/tmpEXTRACTcache/" + args.remote_name + "/"
        args.gzchunks_completed = False #will turn to True if whole file is downloaded
        if args.compressed_bgzip:
            args.gzchunks_fprefix = args.gzcache_dir + "BGZ"
        else:
            args.gzchunks_fprefix = args.gzcache_dir + "GZ"
        if args.verbose:
            how_to_change = "(you can use environment variable TMPDIR to change this location)"
            eprint(" |-- using cache of compressed chunks at '{}' {}".format(args.gzcache_dir,
                                                                             how_to_change))


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
        if args.compressed_gzip:
            find_compressratio() #find how much data was compressed
            if args.remote:
                build_u2c_map() #to build uncompressed to compressed index mapping
        else: #bgzip
            build_u2c_map() #to build uncompressed to compressed index mapping
        #print_u2c_map() #debug
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
            delete_files([chunk_tempfile]) #cleanup


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
                    position, entry_length, identifier, subentry['iv'], \
                        subentry['checksum'] = collated
                else:
                    position, entry_length, identifier, subentry['checksum'] = collated
            elif args.decrypt:
                position, entry_length, identifier, subentry['iv'] = collated
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


def fetch_remote_compressed_entry_gztool(flatfile, position, entry_length, chunknum):
    """
    fetch compressed chunk from url and optionally store it in cache
    """
    chunkstart, chunksize, gzchunk_file, _ = find_chunkstart_size(position,
                                                                  entrysize=entry_length)
    if args.keepcache:
        #final gzchunk_file used may be different than what suggested by find_chunkstart_size
        if not os.path.isfile(gzchunk_file):
            gzchunk_file = retrieve_cached_chunk(chunkstart, chunksize,
                                                 args.chunk_dl_tempfiles[chunknum], gzchunk_file)
        if args.threads != 1:
            open(gzchunk_file+"l", 'a').close() #create lock file: advertise you are reading and to not touch it
        #eprint(" |-- using cached gzchunk file: '{}'".format(gzchunk_file)) #debug
        entry_content = fetch_compressed_entry(gzchunk_file, position, entry_length)
        if args.threads != 1:
            delete_file_if_exists(gzchunk_file+"l") #remove lock file: allow it to be deleted
        return entry_content
    else:
        retrieve_from_size_and_save(args.flatfile,
                                    chunkstart, chunksize,
                                    args.chunk_dl_tempfiles[chunknum])
        return fetch_compressed_entry(args.chunk_dl_tempfiles[chunknum],
                                      position, entry_length)


def fetch_remote_compressed_entry_bgzip(flatfile, position, entry_length, chunknum):
    """
    fetch compressed chunk from url and optionally store it in cache
    """
    chunkstart, chunksize, gzchunk_file, offset = find_chunkstart_size(position, entrysize=entry_length)
    if args.keepcache:
        #find existing chunk(s) from cache
        if os.path.isfile(gzchunk_file):
            if args.threads != 1:
                open(gzchunk_file+"l", 'a').close() #create lock file: advertise you are reading and to not touch it
            with open(gzchunk_file, 'rb') as CHUNKFH:
                bgzf_chunk = read_from_size(CHUNKFH, 0, chunksize)
            if args.threads != 1:
                delete_file_if_exists(gzchunk_file+"l") #remove lock file: allow it to be deleted
        else:
            bgzf_chunk = retrieve_cached_chunk(chunkstart, chunksize,
                                               args.chunk_dl_tempfiles[chunknum], gzchunk_file)
        #uncompress the chunk (still in memory) taking only the part of the entry requested
        return gzip.decompress(bgzf_chunk)[offset:offset+entry_length]
    else: #directly dl and uncompress on the fly without passing from filesystem
        #if args.verbose: #debug
        #   eprint(" |-- going to dl chunk of {} compressed bytes from which we'll extract {} uncompressed entrysize".format(
        #       chunksize, entry_length))
        bgzf_chunk = retrieve_from_size(args.flatfile, chunkstart, chunksize)
    return gzip.decompress(bgzf_chunk)[offset:offset+entry_length]


def fetch_compressed_entry_gztool(flatfile, position, entry_length):
    """
    use gztool to extract a gzip compressed_entry
    from given position of specified flatfile
    """
    gztool_call = [GZTOOL_EXE, '-W', '-b', str(position), '-I', args.gzindex, flatfile]
    #eprint(" |-- calling gztool as '{}'".format(" ".join(gztool_call))) #debug

    with Popen(gztool_call, stdout=PIPE, stderr=DEVNULL) as gztool_pipe:
        return gztool_pipe.stdout.read(entry_length)


def fetch_compressed_entry_bgzip(flatfile, position, entry_length):
    """
    extract a bgzip compressed_entry
    from given position of specified flatfile
    """
    chunkstart, chunksize, _, offset = find_chunkstart_size(position, entrysize=entry_length)
    with open(flatfile, 'rb') as flatfilefh:
        bgzf_chunk = read_from_size(flatfilefh, chunkstart, chunksize)
    return gzip.decompress(bgzf_chunk)[offset:offset+entry_length]


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
            return zip(found_positions, found_lengths, identifiers, found_iv, checksums)
        else:
            return zip(found_positions, found_lengths, identifiers, checksums)
    elif args.decrypt:
        return zip(found_positions, found_lengths, identifiers, found_iv)
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

        #eprint(" |-- #{} extracting '{}' from position '{}' length '{}'".format(
        #   chunknum, identifier, position, entry_length)) #debug

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
            entry['content'] = fetch_remote_compressed_entry(args.flatfile, position,
                                                             entry_length, chunknum)
        else:
            entry['content'] = fetch_compressed_entry(args.flatfile, position, entry_length)

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
    identify entries to extract and collect them, optionally merge together, then extract them
    either single or multithreaded
    """
    global requested_count
    indexfh = open(args.index_filename, 'r', 1)

    if args.verbose:
        eprint(" |-- collecting entry positions")
    if args.merged:
        found_indexes = SortedList() #sorted by integer position
        if args.list_filename:
            with open(args.list_filename) as listfh:
                for line in listfh:
                    found_indexes.update(collect_index(indexfh, line.rstrip()))
                    requested_count[0] += 1
        else:
            for identifier in args.identifiers:
                found_indexes.update(collect_index(indexfh, identifier))
                requested_count[0] += 1
    else:
        found_indexes = list()
        if args.list_filename:
            with open(args.list_filename) as listfh:
                for line in listfh:
                    found_indexes.extend(collect_index(indexfh, line.rstrip()))
                    requested_count[0] += 1
        else:
            for identifier in args.identifiers:
                found_indexes.extend(collect_index(indexfh, identifier))
                requested_count[0] += 1

    indexfh.close()

    #eprint(" found_indexes: {}".format(list(found_indexes))) #debug

    #merge adjacent entries together for faster extraction
    found_length = len(found_indexes)
    if args.merged:
        indexes = merge_adjacent(found_indexes)
        del found_indexes
    else:
        indexes = found_indexes

    #eprint(" indexes: {}".format(indexes)) #debug

    #use positional information from possibly merged indexes to retrieve and print entries
    if indexes: #if not empty
        if args.verbose:
            if args.merged:
                merged_length = len(indexes)
                if merged_length < found_length:
                    eprint(" |-- {} retrievals for {} entries ({} merged)".format(
                        merged_length, found_length, found_length - merged_length))
        if args.threads > 1: #multithread for merged strategy: submit indexes to threads in batches
            indexes_length = len(indexes)
            batchsize = ceil(indexes_length / args.threads)
            batchesnum = ceil(indexes_length / batchsize)

            if args.verbose:
                if args.merged:
                    eprint(" |-- parallel work on {} batches of maximum {} retrievals".format(
                        batchesnum, batchsize))
                else:
                    eprint(" |-- parallel work on {} batches of maximum {} entries".format(
                        batchesnum, batchsize))
            #eprint("batched indexes: {}".format(list(batch(indexes, batchsize)))) #debug

            #init threads
            pool = Pool(args.threads, initializer=init_thread_collectmode, initargs=(
                extracted_count, corrupted_count))
            if args.progressbar:
                _ = list(tqdm(pool.imap(retrieve_entries, batch(indexes, batchsize)),
                              total=batchesnum, ascii=PROGRESSBARCHARS))
            else:
                pool.imap(retrieve_entries, batch(indexes, batchsize))
            pool.close() #no more work to submit
            pool.join() #wait workers to finish
        else:
            retrieve_entries(indexes)


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
        chunknum = int(format(current_process().name.split('-')[1])) - 1 #-1 to start with 0

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


def init_thread_blockmode(r, f, x, c):
    """
    to initialise multithreaded worker
    """
    global requested_count, found_count, extracted_count, corrupted_count, req_positions
    requested_count = r
    found_count = f
    extracted_count = x
    corrupted_count = c


def init_thread_collectmode(x, c):
    """
    to initialise multithreaded worker after indexes already collected
    for merged_adjacent strategy
    """
    global extracted_count, corrupted_count, req_positions
    extracted_count = x
    corrupted_count = c


if __name__ == '__main__':
    check_args()
    #metaprogramming: define accordingly the functions related to two the needed compression scheme
    if args.compressed_gzip: #gztool
        build_u2c_map = build_u2c_map_gztool
        print_u2c_map = print_u2c_map_gztool
        fetch_compressed_entry = fetch_compressed_entry_gztool
        fetch_remote_compressed_entry = fetch_remote_compressed_entry_gztool
        gz_u2b = u2b_gztool
        gz_block_startpos = gz_block_startpos_gztool
        find_chunkstart_size = find_chunkstart_size_common("gztool")
        gzchunks_merge_adjacent = gzchunks_merge_adjacent_common("gztool")
        gzchunks_merge_overlapping = gzchunks_merge_overlapping_common("gztool")
        retrieve_cached_chunk = retrieve_cached_chunk_common("gztool")
        head_extend_gzchunk = head_extend_gzchunk_common("gztool")
        tail_extend_gzchunk = tail_extend_gzchunk_common("gztool")
        retrieve_from_size_and_save = retrieve_from_size_and_save_gztool
    else: #bgzip
        build_u2c_map = build_u2c_map_bgzip
        print_u2c_map = print_u2c_map_bgzip
        fetch_compressed_entry = fetch_compressed_entry_bgzip
        fetch_remote_compressed_entry = fetch_remote_compressed_entry_bgzip
        gz_u2b = u2b_bgzip
        gz_block_startpos = gz_block_startpos_bgzip
        find_chunkstart_size = find_chunkstart_size_common("bgzip")
        gzchunks_merge_adjacent = gzchunks_merge_adjacent_common("bgzip")
        gzchunks_merge_overlapping = gzchunks_merge_overlapping_common("bgzip")
        retrieve_cached_chunk = retrieve_cached_chunk_common("bgzip")
        retrieve_from_size_and_save = retrieve_from_size_and_save_bgzip
        head_extend_gzchunk = head_extend_gzchunk_common("bgzip")
        tail_extend_gzchunk = tail_extend_gzchunk_common("bgzip")

    check_files()

    set_start_method('fork') #spawn not implemented

    requested_count = Array('i', args.chunks_count)
    extracted_count = Array('i', args.chunks_count)
    corrupted_count = Array('i', args.chunks_count)
    found_count = Array('i', args.chunks_count)

    if args.output_filename is None:
        args.outputfh = sys.stdout
    else:
        args.outputfh = open(args.output_filename, 'wb')
    start_secs = time.time()

    if args.threads > 1:
        if args.list_blocksize == 0: #prior collection of indexes then extraction
            collect_extract_entries() #will start multithread after collection
        else: #multithread for normal strategy
            #submit chunks to threads
            #init threads
            pool = Pool(args.threads, initializer=init_thread_blockmode, initargs=(
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
    if args.keepcache:
        eprint(" --- extraction completed: now cleaning and compacting cache")
        cache_cleanup()
