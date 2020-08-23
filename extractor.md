# extractor.py

Use a positional index to retrieve entries from a flatfile, either local or remote,
encrypted or plain text, compressed or uncompressed

Documentation for the whole `ffdb` package: [README](README.md)

Documentation of each utility script:
[indexer](indexer.md)
[extractor](extractor.md)
[remover](extractor.md)
[merger](merger.md)

## In brief:
```bash
extractor.py -f FLATFILE -i INDEXFILE -s IDENTIFIER
#                         or
extractor.py -f FLATFILE -i INDEXFILE -l LISTFILE
#   [-s] : one or more identifiers (space separated)
#   [-l] : filename containing a list of identifiers
    see 'extractor.py -h' for tweaks and optional modes
```

## Notes:
* FLATFILE can be local or remote (use URL as FLATFILE), it can contain
         encrypted or compressed entries, it can even be gzipped as a whole.
         All types will be handled automatically.
* For gzipped FLATFILE, the gztool exe and a GZINDEX need to be available
         see extractor.py -h for tweaks and optional modes

## Examples:
```bash
    extractor.py -f entries.dat -i entries.pos -s Q9GJU7 Q8HYU5 >twoentries.dat
#     (extract the entries identified by Q9GJU7 and Q8HYU5)
    extractor.py -f entries.dat -i entries.pos -d -s 9606 >homo.dat
#     (extract all entries corresponding to identifier 9606)

    extractor.py -f entries.dat -i entries.pos -l chosen.ids >chosen.dat
    extractor.py -f entries.dat.gz -i entries.pos -l chosen.ids -o chosen.dat

    extractor.py -f http://hostname/db -i db.pos -s duffyduck
    extractor.py -f http://hostname/db.gz -i db.pos -I db.gzi -s duffyduck
#     (from a remote db extract entry 'duffyduck': -I needed if gzipped)
```

## Full Usage:
```
  -f FLATFILE, --file FLATFILE
                        filename of flatfile to be processed; can be an URL
  -i INDEX_FILENAME, --index INDEX_FILENAME
                        filename of index file with entry identifiers
  -s IDENTIFIERS [IDENTIFIERS ...], --single IDENTIFIERS [IDENTIFIERS ...]
                        identifier(s) for the desired entry to be extracted
  -t THREADS, --threads THREADS
                        use specified number of multiple threads for parallel retrieval
                        See also `-b` option.
  -l LIST_FILENAME, --list LIST_FILENAME
                        a file containing a list of identifiers for entries to retrieve
  -o OUTPUT_FILENAME, --outfile OUTPUT_FILENAME
                        optionally write output to file rather than to stdout
  -m, --mergedretrieval
                        merge and retrieve together adjacent entries (requires
                        more memory and processing but could prove much faster for
                        remote extraction)
  -v, --verbose         verbose operation
  -d, --duplicates      specify INDEX_FILE could contain duplicate identifiers and
                        request extraction of all of them (default is to extract a
                        single entry)
  -z, --zfound          specify INDEX_FILE contains duplicate identifiers and request
                        extraction of last entry appearing in the flatfile (default is
                        the first)
  -x, --xsanity         check checksums (if provided in index) to confirm sanity of the
                        extracted data (slower extraction)
  -p PASSPHRASE, --passphrase PASSPHRASE
                        specify passphrase to decrypt entries from encrypted flatfile;
                        not recommended on multiuser systems due to security concerns.
                        By default the passphrase will be requested interactively
  -b LIST_BLOCKSIZE, --blocksize LIST_BLOCKSIZE
                        define blocksize (size of LIST_FILENAME chunks) for parallel
                        extraction. This is fast and has lowest memory requirements,
                        but could be less efficient if lots of entries are mapped to
                        same identifier when --duplicates is used. If unspecified,
                        it will be adjusted automatically to number of threads.
                        Use `-b 0` to disable block extraction

  -c, --compressed_gzip
                        specify flatfile is gzipped; a .gzi GZINDEX file is required
  -C, --Compressed_bgzip
                        specify flatfile is bgzipped; a .gzi GZINDEX file is required if flatfile is
                        remote

  -g GZINDEX, --gzindex GZINDEX
                        filename of the compressed index .gzi
  -r, --remote          specify flatfile is remote and treat FLATFILE as an URL
  -k, --keepcache       keep local cache to consume less bandwidth; only applicable to
                        remote gzipped flatfiles
```

## Copyright

`ffdb` is licensed under the [GNU Affero General Public License](https://choosealicense.com/licenses/agpl-3.0/).

(c) Copyright [Giuseppe Insana](http://insana.net), 2020-
