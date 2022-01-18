# remover.py

Delete entries from a flat-file database.

Documentation for the whole `ffdb` package: [README](README.md)

Documentation of each utility script:
[indexer](indexer.md)
[extractor](extractor.md)
[remover](extractor.md)
[merger](merger.md)

## In brief:
```bash
remover.py -f FLATFILE -i INDEXFILE -l LISTFILE [-o OUTPATH]
#   [-f] : flatfile from which the entries should be removed
#   [-i] : index of FLATFILE
#   [-l] : file with list of identifers for the entries that should be removed
```

## Notes:
  * For safety reasons, new flatfile and index files are created, rather than touching
    the original files. It is left to the user the final moving and replacement
    of old files with new ones.

## Examples:
```bash
    remover.py -f entries.dat -i entries.pos -l removeme.list
#     (will create entries.dat.new and entries.pos.new)
    remover.py -f entries.dat -i entries.pos -l removeme.list -o cleaned
#     (will create cleaned/entries.dat.new and cleaned/entries.pos.new)
```

## Full Usage:
```
  -f FLATFILE, --file FLATFILE
                        filename of flatfile from which entries should be deleted
  -i INDEX_FILENAME, --index INDEX_FILENAME
                        filename of index file containing entry identifiers
  -l LIST_FILENAME, --list LIST_FILENAME
                        a file containing a list of identifiers corresponding to
                        entries to delete
  -o OUTPATH, --outpath OUTPATH
                        write new files to specified path rather than creating new
                        files in the same location as the original ones
  -v, --verbose         verbose operation
  -d, --duplicates      specify INDEX_FILE could contain duplicate identifiers and
                        request deletion of all of them (default is to delete a single
                        entry)
  -z, --zfound          specify INDEX_FILE contains duplicate identifiers and request
                        deletion of last entry appearing in the flatfile (default is
                        the first)
  -t THREADS, --threads THREADS
                        use specified number of multiple threads for parallel
                        reindexing
  -b INDEX_BLOCKSIZE, --blocksize INDEX_BLOCKSIZE
                        redefine blocksize used for parallel execution. By default it
                        will be adjusted automatically to the number of threads
```

## Copyright

`ffdb` is licensed under the [GNU Affero General Public License](https://choosealicense.com/licenses/agpl-3.0/).

(c) Copyright [Giuseppe Insana](http://insana.net), 2020-
