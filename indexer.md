# indexer.py

Create a positional index for any flatfile, optionally compressing or
encrypting its entries

Documentation for the whole `ffdb` package:
* [GitHub page](https://github.com/g-insana/ffdb.py/)

Documentation of each utility script:
* [indexer](indexer.md)
* [extractor](extractor.md)
* [remover](extractor.md)
* [merger](merger.md)


## Usage
```bash
indexer.py -f FLATFILE -i 'PATTERN' [-e ENDPATTERN] >INDEXFILE
    [-f] : flatfile to index
    [-i] : regex pattern for the identifiers; also [-j], see examples below
    [-e] : pattern for end of entry. defaults to "^-$"
```

## Notes:
  * If compression or encryption is requested, an output flatfile will
         be created, and the resulting index will refer to it.
  * If the identifiers are a LOT and memory is an issue, you may wish to use
         [-u] option and sort the resulting index after it has been generated.

## Examples:
```bash
       indexer.py -i '^AC   (.+?);' -f uniprot.dat -e '^//$' >up.pac
       indexer.py -i '^AC   (.+?);' 'ID   (.+?);' -f [...]
#        (multiple patterns can be specified)

       indexer.py -i '^AC   (.+?);' -j '^OX   NCBI_(Tax)ID=(\d+) ' -f [...]
#        (complex patterns made of multiple parts can be specified with [-j];
#         -i and -j patterns can be used together)

       indexer.py -a -j '^DR   (.+?);( .+?);' -f [...]
       indexer.py -a -i '^AC   (\S+?); ?(\S+)?;? ?(\S+)?;?' -f [...]
#        (use [-a] option to find all instances and capture groups of
#         the provided patterns, not just the first one)
```

# Full Usage:
  -f INPUT_FILENAME, --file INPUT_FILENAME
                        Filename of flatfile to be processed
  -i PATTERNS [PATTERNS ...], --id PATTERNS [PATTERNS ...]
                        regexp pattern for identifier(s) to index
  -j JOINEDPATTERNS [JOINEDPATTERNS ...], --joinedid JOINEDPATTERNS [JOINEDPATTERNS ...]
                        regexp pattern for identifier(s) to index
  -e TERMINATOR, --endpattern TERMINATOR
                        regexp pattern to identify the end of each entry. If
                        unspecified it defaults to '^-$'
  -a, --allmatches      find all instances of the identifier pattern, not just
                        the first one (the default behaviour)
  -v, --verbose         verbose operation
  -t THREADS, --threads THREADS
                        use specified number of threads for parallel indexing
  -b INPUT_BLOCKSIZE, --blocksize INPUT_BLOCKSIZE
                        redefine blocksize used for parallel execution. By
                        default it will be adjusted automatically to the
                        number of threads
  -o POS_OFFSET, --offset POS_OFFSET
                        optional offset (in bytes) to shift entry positions in
                        index
  -k {16,24,32}, --keysize {16,24,32}
                        request entries to be encrypted and specify encryption
                        strength: 16=aes-128, 24=aes-192 or 32=aes-256.
                        INPUT_FILENAME.enc will be created
  -p PASSPHRASE, --passphrase PASSPHRASE
                        passphrase for encrypting the entries; if unspecified
                        it will be requested interactively (safer)
  -c {0,1,2,3,4,5,6,7,8,9}, --compresslevel {0,1,2,3,4,5,6,7,8,9}
                        request entries to be compressed and specify a
                        compress level. INPUT_FILENAME.xz will be created
  -x, --xsanity         compute entry checksums and add them to index
  -u, --unsorted        do not sort the index, leaving that task to a followup
                        external command. Note that extraction requires a
                        sorted index
  -n, --nopos           do not compute positions, just print matching
                        identifiers

## Copyright

`ffdb` is licensed under the [GNU Affero General Public License](https://choosealicense.com/licenses/agpl-3.0/).

(c) Copyright [Giuseppe Insana](http://insana.net), 2020-
