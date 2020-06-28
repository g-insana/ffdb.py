#!/usr/bin/env python3
"""
ffdb
This module and the associated set of utility scripts
allow the creation, maintenance and usage of a database and document storage
which employs a single container file.

by Giuseppe Insana, 2020
"""
import os
import re
import sys
import time
import zlib
from math import ceil
from heapq import merge #alternative mergesort
from Cryptodome.Cipher import AES
from Cryptodome.Protocol import KDF
from Cryptodome import Random

# pylint: disable=C0103,R0912,R0915,W0603
VERSION = '2.4.2'

##CUSTOMIZATIONS:

#personalize this program by modifying the
#following SALT used by the PBKDF2 routine
SALT = b'5ed3a4284d6a9c1e4e4f6b4729b254be'

#change the following if you prefer space or other delimiter for the index
FIELDSEPARATOR = "\t"

#define temporary directory in case no TMPDIR is specified
if 'TMPDIR' in os.environ and os.environ['TMPDIR'] != '':
    TEMPDIR = os.environ['TMPDIR']
else:
    TEMPDIR = "/tmp"

#customize the progress bar (for multithread verbose progress)
#PROGRESSBARCHARS = False #use this for standard unicode smooth blocks
PROGRESSBARCHARS = ' ·•'

#only relevant for extraction via gztool
#from local or remote compressed flatfiles;
#what is the full path of gztool:
GZTOOL_EXE = None
#e.g.:
#GZTOOL_EXE = "/usr/local/bin/gztool"
#if not hardcoded here, be sure it is found in your PATH

#buffer size used by io operations
#BUFFERSIZE = 8192
BUFFERSIZE = 1048576

##CONSTANTS:

REIV = re.compile(r'^([0-9a-zA-Z{}]+)\.([0-9a-zA-Z{}]+)\|([A-Z])(.+)$') #only encrypted
REIVD = re.compile(r'^([0-9a-zA-Z{}]+)\+([0-9a-zA-Z{}]+)\|([A-Z])(.+)$') #deflated and encrypted
REESIV = re.compile(r'^([0-9a-zA-Z{}]+)(?:\.|\+)([0-9a-zA-Z{}]+)\|[A-Z](.+)$') #entrysize+iv stored
REESIVN = re.compile(r'^([0-9a-zA-Z{}]+)(?:\.|\+)([0-9a-zA-Z{}]+\|[A-Z].+)$') #don't capture iv
REES = re.compile(r'^([0-9a-zA-Z{}]+)(?::|-)([0-9a-zA-Z{}]+)$') #entrysize but no iv stored
RETERMINATOR = re.compile(r'^-$')

B64_CHARS = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ{}'
B64_INDEX = dict((char, pos) for pos, char in enumerate(tuple(B64_CHARS)))


def calculate_chunknum(filename, blocksize):
    """
    calculate number of chunks from global defined blocksize
    and file's size
    """
    filesize = os.path.getsize(filename)
    number_of_chunks = filesize // blocksize + 1
    #eprint("filename {} filesize {} blocksize {}".format(filename, filesize, blocksize)) #debug
    return filesize, number_of_chunks


def calculate_blocksize(filename, number_of_chunks):
    """
    calculate size of a single block
    from number of chunk/splits desired and a file's filesize
    """
    filesize = os.path.getsize(filename)
    if filesize == 0:
        raise RuntimeError("File '{}' is empty".format(filename))
    blocksize = int(ceil(filesize / number_of_chunks))
    number_of_chunks = int(ceil(filesize / blocksize))
    return blocksize


def find_next_delimiter(fp, filesize, startpos, delimiter, buffersize=None):
    """
    return the position of first delimiter found after given starting position
    """
    if startpos > filesize:
        return filesize
    if buffersize is None:
        buffersize = 16384
    delimiterlen = len(delimiter)
    fp.seek(startpos)
    data = fp.read(buffersize)
    while data:
        delimiterpos = data.find(delimiter)
        #eprint("{}-{} checking for {}: {}".format(startpos, fp.tell(), delimiter, delimiterpos))
        if delimiterpos != -1:
            return startpos + delimiterpos + delimiterlen
        startpos += buffersize
        data = fp.read(buffersize)
    return filesize #could not find


def find_next_newline(fp, filesize, startpos):
    """
    return the position of first newline found after given starting position
    """
    if startpos > filesize:
        return filesize
    if startpos <= 0:
        return 0
    fp.seek(startpos - 1)
    fp.readline() # to avoid truncatedlines
    return fp.tell()


def chunk_startpos_onnewline(fp, filesize, blocksize, block):
    """
    return the initial byte of a chunk without breaking newlines
    """
    ini = block * blocksize
    end = ini + blocksize
    if ini > filesize:
        return None
    if end > filesize:
        end = filesize
    if ini <= 0:
        fp.seek(0)
    else:
        fp.seek(ini - 1)
        fp.readline() # to avoid truncatedlines
    return fp.tell()


def chunk_of_lines(fp, filesize, blocksize, block):
    """
    return a generator of the lines coming from a single chunk of the file
    """
    ini = block * blocksize
    end = ini + blocksize
    if ini > filesize:
        return
    if end > filesize:
        end = filesize
    if ini <= 0:
        fp.seek(0)
    else:
        fp.seek(ini - 1)
        fp.readline() # to avoid truncatedlines
    while fp.tell() < end:
        yield fp.readline()


def int_to_b64(number):
    """
    convert a decimal integer to a base64encoded number
    """
    if number == 0:
        return '0'
    string = ''
    while number:
        string = B64_CHARS[number % 64] + string
        number //= 64
        #~13% slower:
        #number, remainder = divmod(number, 64)
        #string = B64_CHARS[remainder] + string
    return string


def b64_to_int(string):
    """
    convert a base64encoded number to decimal integer
    """
    strlen = len(string)
    number = 0
    index = 0
    for char in string:
        index += 1
        power = strlen - index
        number += B64_INDEX[char] * (64 ** power)
        #~20% slower:
        # number += B64_CHARS.index(char) * (64 ** power)
    return number


def format_indexes(entry, index_type, cipher_type, checksums=False):
    """
    return array of formatted indexes
    """
    fieldseparator = FIELDSEPARATOR
    indexes = list()
    #if len(entry['ids']) == 0:
    #   eprint("processing entry '{}'".format(entry['full']))
    if index_type == "":
        for identifier in entry['ids']:
            indexes.append("{}\n".format(identifier))
        return indexes

    length = entry['length_b64']
    if index_type in (".", "+"): #encryption: the iv is part of the index
        entryposition = "{}{}{}|{}{}".format(entry['position'],
                                             index_type,
                                             length,
                                             cipher_type,
                                             entry['iv'].hex())
    else: #no iv
        entryposition = "{}{}{}".format(entry['position'],
                                        index_type,
                                        length)
    for identifier in entry['ids']:
        if checksums:
            indexes.append("{1}{0}{2}{0}{3}\n".format(fieldseparator,
                                                      identifier,
                                                      entryposition,
                                                      int_to_b64(entry['checksum'])))
        else:
            indexes.append("{}{}{}\n".format(identifier, fieldseparator, entryposition))
    return indexes


def deflate(data, compresslevel=9):
    """
    compress a byte object
    """
    compress = zlib.compressobj(
        compresslevel,        # level: 0-9
        zlib.DEFLATED,        # method: must be DEFLATED
        -zlib.MAX_WBITS,      # window size in bits:
                              #   -15..-8: negate, suppress header
                              #   8..15: normal
                              #   16..30: subtract 16, gzip header
        zlib.DEF_MEM_LEVEL,   # mem level: 1..8/9
        0                     # strategy:
                              #   0 = Z_DEFAULT_STRATEGY
                              #   1 = Z_FILTERED
                              #   2 = Z_HUFFMAN_ONLY
                              #   3 = Z_RLE
                              #   4 = Z_FIXED
    )
    deflated = compress.compress(data)
    deflated += compress.flush()
    return deflated


def inflate(data):
    """
    decompress a byte object
    """
    decompress = zlib.decompressobj(
        -zlib.MAX_WBITS
    )
    inflated = decompress.decompress(data)
    inflated += decompress.flush()
    return inflated


def get_cipher_type(cipher):
    """
    return cipher_type letter from cipher
    """
    cipher2letter = {'aes128': 'A', 'aes192': 'B', 'aes256': 'C'}
    if cipher not in cipher2letter:
        eprint("    => ERROR: cipher '{}' not recognised!".format(cipher))
        sys.exit(1)
    return cipher2letter[cipher]


def find_keysize_cipher(cipher_type):
    """
    infer cipher and keysize from cipher_type letter
    return keysize and cipher
    """
    if cipher_type == "A":
        cipher_name = "aes128"
        keysize = 16
    elif cipher_type == "B":
        cipher_name = "aes192"
        keysize = 24
    elif cipher_type == "C":
        cipher_name = "aes256"
        keysize = 32
    else:
        eprint("    => ERROR: cipher_type '{}' not recognised!".format(cipher_type))
        sys.exit(1)
    return keysize, cipher_name


def merge_indexes(index1_filename, index2_filename, pos_offset,
                  index_type, outfile, tempfile=None):
    """
    merge two index files into one outfile, shifting the second one of given offset
    returns number of identifiers from the second index
    """
    if tempfile is None: #read second index into array, then merge
        new_identifiers = shift_index(index2_filename,
                                      pos_offset,
                                      index_type)
        new_identifiers_count = len(new_identifiers)
        #faster than heap_mergesort if len(new_identifiers) is small
        mergesort_file_array(index1_filename, new_identifiers, outfile)
        #slower but with good performance no matter which size:
        #heap_mergesort_file_array(index1_filename, new_identifiers, outfile) #ALT
    else: #use a temporary file to shift the second index, then merge files
        new_identifiers_count = shift_index_file(index2_filename,
                                                 pos_offset,
                                                 tempfile,
                                                 index_type)
        mergesort_files([index1_filename, tempfile], outfile)
        os.remove(tempfile)
    return new_identifiers_count


def mergesort_file_array(filename, insertions, outfilename):
    """
    merge sort contents of a file and an array
    writing to outputfile
    """
    newline = insertions.pop(0)
    with open(outfilename, 'w') as outfh:
        with open(filename, 'r', 1) as sortedfh: #using line buffering
            for line in sortedfh:
                while insertions:
                    if newline <= line:
                        outfh.write(newline)
                        newline = insertions.pop(0) #prepare for next
                    else:
                        break
                if newline <= line:
                    outfh.write(newline)
                    newline = ""
                outfh.write(line)
            if newline != "":
                outfh.write(newline)
            if insertions: #if some left in the end, append
                for newline in insertions:
                    outfh.write(newline)


def heap_mergesort_file_array(filename, insertions, outfilename):
    """
    use heapq.merge to merge sort contents of a file and an array
    writing to outputfile
    """
    with open(outfilename, 'w') as outfh:
        with open(filename, 'r', 1) as sortedfh: #using line buffering
            outfh.writelines(merge(sortedfh, insertions))


def mergesort_files(filenames, outfilename):
    """
    use heapq.merge to merge sort contents of files
    writing to outputfile
    """
    filehandles = list()
    for filename in filenames:
        filehandles.append(open(filename, 'r', 1))
    with open(outfilename, 'w') as outfh:
        outfh.writelines(merge(*filehandles))
    for filehandle in filehandles:
        filehandle.close()


def file_string_binary_search(fh, string, mode=None):
    """
    perform a binary_search for a string in a sorted text file
    return all matches, unless mode specified (can be 'first' or 'last')
    """
    found = None
    foundpos = None
    lo = 0
    fh.seek(0, 2)
    hi = fh.tell() - 1 #filesize
    while lo < hi:
        mid = (hi + lo) >> 1
        if mid > 0:
            fh.seek(mid - 1)
            fh.readline() #to avoid truncated line we skip this one
            midf = fh.tell()
        else:
            midf = 0
            fh.seek(midf)
        line = fh.readline() #read next line
        #eprint("lo {} hi {} mid {} midf {}".format(lo, hi, mid, midf)) #debug
        if not line or string <= line:
            hi = mid
        else:
            lo = mid + 1
    if mid == lo: #range fully scanned
        foundpos = midf
    elif lo <= 0: #very beginning
        foundpos = 0
    if foundpos is None:
        line = fh.readline()
        #foundpos = fh.tell()
    #eprint("got fist match at pos {}".format(foundpos))
    if mode == 'first': # return earliest match
        if line.startswith(string):
            found = line
    elif mode == 'last': # return latest match
        while line.startswith(string): #continue skipping lines while they match
            found = line
            line = fh.readline()
    else: #multi: return all matches
        found = list()
        while line.startswith(string): #continue adding lines while they match
            found.append(line)
            line = fh.readline()
    return found


def _file_string_binary_search_any(fh, string):
    """
    perform a binary_search for a string in a sorted text file
    it will quickly return any match, not necessarily the first or the last in the file
    """
    found = None
    #start checking the very first line:
    fh.seek(0)
    line = fh.readline()
    if line.startswith(string):
        found = line
    else:
        lo = 0 #beginning of file
        fh.seek(0, 2)
        hi = fh.tell() #end of file
        while hi - lo > 1:
            mid = (hi + lo) // 2
            fh.seek(mid, 0)
            fh.readline() #to avoid truncated line we skip this one
            line = fh.readline() #read next line
            if line.startswith(string):
                found = line
                break
            if string > line:
                lo = mid
            else:
                hi = mid
        if found is None:
            fh.seek(hi)
            line = fh.readline()
            if line.startswith(string):
                found = line

    if found is not None:
        return found
    return None


def get_position_first(indexfh, identifier):
    """
    find from the index the position of the entry
    corresponding to specified identifier
    """
    identifier += FIELDSEPARATOR #e.g. "ABCDE\t"
    line = file_string_binary_search(indexfh, identifier, mode='first')
    if line:
        columns = line.rstrip().split(FIELDSEPARATOR)
        return columns[1]
    return None #if could not be found


def get_position_last(indexfh, identifier):
    """
    find from the index the position of the entry
    corresponding to specified identifier
    """
    identifier += FIELDSEPARATOR #e.g. "ABCDE\t"
    line = file_string_binary_search(indexfh, identifier, mode='last')
    if line:
        columns = line.rstrip().split(FIELDSEPARATOR)
        return columns[1]
    return None #if could not be found


def get_positions(indexfh, identifier):
    """
    find from the index all the positions
    corresponding to (possibly duplicated) specified identifier
    """
    positions = list()
    identifier += FIELDSEPARATOR #e.g. "ABCDE "
    lines = file_string_binary_search(indexfh, identifier)
    if lines:
        for line in lines:
            columns = line.rstrip().split(FIELDSEPARATOR)
            positions.append(columns[1]) #2nd column of index file is position
    return positions


def get_position_checksum_first(indexfh, identifier):
    """
    find from the index the position of the entry
    corresponding to specified identifier
    additionally return checksum
    """
    identifier += FIELDSEPARATOR #e.g. "ABCDE\t"
    line = file_string_binary_search(indexfh, identifier, mode='first')
    if line:
        columns = line.rstrip().split(FIELDSEPARATOR)
        return columns[1], columns[2]
    return None #if could not be found


def get_position_checksum_last(indexfh, identifier):
    """
    find from the index the position of the entry
    corresponding to specified identifier
    additionally return checksum
    """
    identifier += FIELDSEPARATOR #e.g. "ABCDE\t"
    line = file_string_binary_search(indexfh, identifier, mode='last')
    if line:
        columns = line.rstrip().split(FIELDSEPARATOR)
        return columns[1], columns[2]
    return None #if could not be found


def get_positions_checksums(indexfh, identifier):
    """
    find from the index all the positions
    corresponding to (possibly duplicated) specified identifier
    """
    positions = list()
    checksums = list()
    identifier += FIELDSEPARATOR #e.g. "ABCDE "
    lines = file_string_binary_search(indexfh, identifier)
    if lines:
        for line in lines:
            columns = line.rstrip().split(FIELDSEPARATOR)
            positions.append(columns[1]) #2nd column of index file is position
            checksums.append(columns[2]) #3rd column of index file is checksum
    return positions, checksums


def init_cipher(key, iv):
    """
    initialize and return a new cipher
    """
    return AES.new(key, AES.MODE_CFB, iv)


def generate_iv():
    """
    generate and return a new iv
    """
    return Random.new().read(AES.block_size)


def derive_key(passphrase, keysize):
    """
    derive key of desired bytesize from passphrase
    """
    key = KDF.PBKDF2(passphrase, SALT, keysize)
    if len(key) != keysize:
        msg = "Attention! Something has gone wrong when deriving key from passphrase!"
        raise RuntimeError(msg)
    ks2cipher = {16: 'aes128', 24: 'aes192', 32: 'aes256'}
    cipher_name = ks2cipher[keysize]
    return cipher_name, key


def check_index(line):
    """
    read first record of a positional index and gather needed information
    return index_type, cipher_name, keysize, has_checksums
    """
    cipher_name = None
    keysize = None
    has_checksums = False

    if line.find(FIELDSEPARATOR) == -1:
        eprint("    => ERROR: index does not have positional column. Wrong file??")
        sys.exit(1)

    columns = line.split(FIELDSEPARATOR)
    if len(columns) > 2: #3rd column is crc
        has_checksums = True
    position = columns[1] #2nd column of index file
    iv = None
    if position.find("+") != -1: #both compressed and encrypted
        index_type = "+"
        iv = REIVD.match(position)
    elif position.find(".") != -1: #encrypted but not compressed
        index_type = "."
        iv = REIV.match(position)
    elif position.find(":") != -1: #compressed but not encrypted
        index_type = ":"
    elif position.find("-") != -1: #not encrypted nor compressed but entry sizes stored
        index_type = "-"
    else:
        eprint("    => ERROR: index does not have index_type identifier. Wrong file??")
        sys.exit(1)

    if index_type in (".", "+"): #encrypted entries
        if iv is None:
            eprint("    => ERROR: missing iv, cannot infer cipher type and keysize!")
            sys.exit(1)
        cipher_type = iv.group(3) #A, B, C..
        keysize, cipher_name = find_keysize_cipher(cipher_type)
    return index_type, cipher_name, keysize, has_checksums


def read_from_size(fh, begin, size):
    """
    reads from filehandle a specified amount of bytes
    """
    fh.seek(begin)
    return fh.read(size)


def delete_files(filenames):
    """
    delete the temporary sub-files
    """
    for filename in filenames:
        if os.path.isfile(filename):
            os.remove(filename)


def close_subfiles(fhlist):
    """
    close the filehandles for the temporary sub-files
    """
    for fh in fhlist:
        fh.close()


def print_subfiles(filenames, fh=None):
    """
    read and print a series of temporary sub-files writing them sequentially
    to a given output filehandle or print them to stdout
    """
    mode = 'rb'
    if fh is None:
        fh = sys.stdout #print to stdout if no fh given
        mode = 'r'
    for filename in filenames:
        with open(filename, mode) as infile:
            ##straight, unbuffered, will read whole file in memory
            #fh.write(infile.read())
            ##buffered:
            filesize = os.path.getsize(filename)
            blockcount, remainder = divmod(filesize, BUFFERSIZE)
            for _ in range(blockcount):
                buffered = infile.read(BUFFERSIZE)
                fh.write(buffered)
            fh.write(infile.read(remainder))


def shift_index(index_filename, offset, index_type):
    """
    shift positions in index file by given offset
    return list with identifiers/positions
    """
    identifiers = list()
    with open(index_filename, 'r', 1) as indexfh: #using line buffering
        for line in indexfh:
            columns = line.split(FIELDSEPARATOR)
            position = columns[1] #2nd column of index file is position
            if index_type in ("+", "."): #entry sizes and iv stored
                posmatch = REESIVN.match(position)
            elif index_type in (":", "-"): #entry sizes stored, no iv
                posmatch = REES.match(position)
            position, remaining = posmatch.groups()
            position = b64_to_int(position)
            columns[1] = "{}{}{}".format(int_to_b64(position + offset),
                                         index_type,
                                         remaining)
            identifiers.append(FIELDSEPARATOR.join(columns) + "\n")
    return identifiers


def shift_index_file(index_filename, offset, outfile, index_type):
    """
    shift positions in index file by given offset creating outfile
    """
    buffered = ""
    shifted_count = 0
    buflines = 100 #write every 100 lines
    bufcounter = 0
    with open(index_filename, 'r', 1) as indexfh: #using line buffering
        with open(outfile, 'w') as outfh:
            for line in indexfh:
                columns = line.split(FIELDSEPARATOR)
                position = columns[1] #2nd column of index file is position
                if index_type in ("+", "."): #entry sizes and iv stored
                    posmatch = REESIVN.match(position)
                elif index_type in (":", "-"): #entry sizes stored, no iv
                    posmatch = REES.match(position)
                position, remaining = posmatch.groups()
                position = b64_to_int(position)
                if bufcounter > buflines: #write to outfile and reset buffer
                    outfh.write(buffered)
                    shifted_count += bufcounter
                    bufcounter = 0
                    buffered = ""
                columns[1] = "{}{}{}".format(int_to_b64(position + offset),
                                             index_type,
                                             remaining)
                buffered += FIELDSEPARATOR.join(columns) + "\n"
                bufcounter += 1
            shifted_count += bufcounter
            outfh.write(buffered) #last block
    return shifted_count


def encode_delimiter_re(delimiter):
    """
    encode a delimiter regexp, replacing any ^ and $ with newlines
    """
    if delimiter[0] == "^" or delimiter[-1] == "$":
        delimiter = list(delimiter)
        if delimiter[0] == "^":
            delimiter[0] = "\n"
        if delimiter[-1] == "$":
            delimiter[-1] = "\n"
        delimiter = "".join(delimiter)
    return "{}".format(delimiter).encode('UTF-8')


def postprocess_entry(entry, mode):
    """
    perform any compression, encryption, checksum calculation as needed
    """
    if mode['xsanity']:
        entry['checksum'] = zlib.crc32(entry['full'])
    if mode['encrypt']:
        entry['iv'] = generate_iv() #generate and store new iv
        cipher = init_cipher(mode['key'], entry['iv'])
        if mode['compress']:
            entry['full'] = cipher.encrypt(deflate(entry['full'],
                                                   compresslevel=mode['compresslevel']))
            entry['length'] = len(entry['full']) #probably smaller, need to recompute
        else:
            entry['full'] = cipher.encrypt(entry['full'])
        mode['outffh'].write(entry['full'])
    elif mode['compress']: #compressed but not encrypted
        entry['full'] = deflate(entry['full'], compresslevel=mode['compresslevel'])
        entry['length'] = len(entry['full']) #probably smaller, need to recompute
        mode['outffh'].write(entry['full'])
    entry['length_b64'] = int_to_b64(entry['length'])


def entry_generator(path, delimiter, buffersize=None):
    """
    splits a file according to a delimiter, returning a generator of entries
    terminating in the delimiter
    """
    delimiter = encode_delimiter_re(delimiter)
    splen = len(delimiter)
    buf = b''
    with open(path, 'rb') as fh:
        data = fh.read(buffersize)
        while data:
            #buf += fh.read(buffersize)
            buf += data
            start = end = 0
            while end >= 0:# and len(buf)>=splen:
                start, end = end, buf.find(delimiter, end)
                if end != -1:
                    end += splen
                    yield buf[start:end], end - start
                else:
                    buf = buf[start:]
                    break
            data = fh.read(buffersize)
        #yield buf #if final content after terminator is desired


def compute_split_positions(filename, splitsize, delimiter=None):
    """
    compute and return start positions and sizes of chunks of splitsize
    without breaking lines or without breaking entries (if delimiter provided)
    """
    split_file_sizes = [0] #init with 0 size (this will be removed later)
    input_filesize = os.path.getsize(filename)

    split_file_startpos = [0] #first split start position is 0 (start of file)
    split_file_sizes = list() #init
    if delimiter is None:
        with open(filename, 'r', 1) as inputfh:
            split_file_end = find_next_newline(inputfh, input_filesize,
                                               splitsize)
            split_file_sizes.append(split_file_end)
            while split_file_end != input_filesize:
                split_file_start = split_file_end #of previous block
                split_file_startpos.append(split_file_start)
                split_file_end = find_next_newline(inputfh, input_filesize,
                                                   split_file_start + splitsize)
                split_file_sizes.append(split_file_end - split_file_start)
    else:
        delimiter = encode_delimiter_re(delimiter)
        bufsize = max(1024, min(1024*splitsize//65536, 1024*1024))
        with open(filename, 'rb', 1) as inputfh:
            split_file_end = find_next_delimiter(inputfh, input_filesize,
                                                 splitsize, delimiter, bufsize)
            split_file_sizes.append(split_file_end)
            while split_file_end != input_filesize:
                split_file_start = split_file_end #of previous block
                split_file_startpos.append(split_file_start)
                split_file_end = find_next_delimiter(inputfh, input_filesize,
                                                     split_file_start + splitsize,
                                                     delimiter, bufsize)
                split_file_sizes.append(split_file_end - split_file_start)

    return split_file_startpos, split_file_sizes


def split_file(filename, splitsize, outprefix, delimiter=None):
    """
    split a file into chunks of splitsize without breaking lines
    returns list of chunk filenames and list of the chunk filesizes
    """
    split_files = list()
    split_file_startpos, split_file_sizes = compute_split_positions(filename, splitsize, delimiter)

    split_file_count = len(split_file_sizes)
    suffixlength = len(str(split_file_count))

    with open(filename, 'rb') as inputfh:
        for chunknum in range(split_file_count):
            chunk_suffix = str(chunknum).zfill(suffixlength)
            splitfile = outprefix + chunk_suffix
            split_files.append(splitfile)
            with open(splitfile, 'wb') as outfh:
                inputfh.seek(split_file_startpos[chunknum])
                ##straight, unbuffered, will read whole file in memory:
                #outfh.write(inputfh.read(split_file_sizes[chunknum]))
                ##buffered:
                blockcount, remainder = divmod(split_file_sizes[chunknum], BUFFERSIZE)
                for _ in range(blockcount):
                    buffered = inputfh.read(BUFFERSIZE)
                    outfh.write(buffered)
                outfh.write(inputfh.read(remainder))
    return split_files, split_file_sizes


def buffered_readwrite(inputfile, outputfile, startpos, size):
    """
    extract a piece of inputfile of given size from given position
    and write it to outputfile
    """
    with open(inputfile, 'rb') as inputfh:
        with open(outputfile, 'wb') as outfh:
            inputfh.seek(startpos)
            blockcount, remainder = divmod(size, BUFFERSIZE)
            for _ in range(blockcount):
                buffered = inputfh.read(BUFFERSIZE)
                outfh.write(buffered)
            outfh.write(inputfh.read(remainder))
            outfh.flush()


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


def siprefix2num(numberstring):
    """
    Returns bytes from a metrix suffixed string
    """
    if numberstring == '0':
        return(0)

    prefix = {"k": 1024, "m": 1024**2, "g": 1024**3, "t": 1024**4, "p": 1024**5}
    numberstring = numberstring.lower()
    try:
        return int(numberstring[:-1]) * prefix[numberstring[-1]]
    except KeyError:
        # no or unknown meter-prefix
        return int(numberstring)


def elapsed_time(start_time, work_done=None):
    """
    compute elapsed time from given start_time (in seconds) and return
    well formatted string. If work_done argument given, compute speed of the process
    """
    process_time = time.time() - start_time
    minutes, seconds = divmod(process_time, 60)
    hours, minutes = divmod(minutes, 60)
    if work_done is None:
        return("{:02.0f}h {:02.0f}m {:02.0f}s".format(
            hours, minutes, seconds))
    process_speed = str(round(work_done / process_time, 2))
    return "{:02.0f}h {:02.0f}m {:02.0f}s".format(hours, minutes, seconds), process_speed


def eprint(*myargs, **kwargs):
    """
    print to stderr, useful for error messages and to not clobber stdout
    """
    print(*myargs, file=sys.stderr, **kwargs)
