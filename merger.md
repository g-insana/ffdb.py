# merger.py

Merge new pre-indexed entries into an existing flat-file database

Documentation for the whole `ffdb` package: [README](README.md)

Documentation of each utility script:
[indexer](indexer.md)
[extractor](extractor.md)
[remover](extractor.md)
[merger](merger.md)

## In brief:
```bash
merger.py -f FLATFILE -i INDEXFILE -e ENTRIESFILE -n NEWINDEXFILE
#   [-f] : flatfile into which the new entries should be added
#   [-i] : index of FLATFILE
#   [-e] : filename containing the new entries to be added
#   [-n] : index of ENTRIESFILE
```

## Examples:
```bash
       merger.py -f db.dat -i db.pos -e new.dat -n new.pos
#        (will update db.dat and db.pos)
       merger.py -c -f db.dat -i db.pos -e new.dat -n new.pos
#        (will create db.dat.new and db.pos.new)
       merger.py -c -o export -f db.dat -i db.pos -e new.dat -n new.pos
#        (will create export/db.dat.new and export/db.pos.new)
```

## Notes:
* Usually the script is used to encompass a smaller newentries db into a larger and growing db, but this script can also be used to merge two databases of equal size.

## Full Usage:
```
  -f FF_FILENAME, --file FF_FILENAME
                        filename of flatfile to be processed
  -i INDEX_FILENAME, --index INDEX_FILENAME
                        filename of index file with entry identifiers
  -e NEWENTRIES_FILENAME, --entries NEWENTRIES_FILENAME
                        filename of new entries to be merged into flatfile
  -n NEWINDEX_FILENAME, --newindex NEWINDEX_FILENAME
                        filename of index file with entry identifiers
  -c, --create          create new files (.new extension) rather than updating existing
                        files (the default operation mode)
  -o OUTPATH, --outpath OUTPATH
                        optionally write new files to specified path rather than
                        creating new files in the same location as the original ones
  -v, --verbose         verbose operation
  -d, --delete          delete ENTRIESFILE and NEWINDEXFILE after merging is completed
  -g, --gzip            compress the final flatfile after merge, creating .gzi
                        compressed index
  -s, --small           use this mode if the new index is small (<30k entries):
                        performance should be better
```

## Copyright

`ffdb` is licensed under the [GNU Affero General Public License](https://choosealicense.com/licenses/agpl-3.0/).

(c) Copyright [Giuseppe Insana](http://insana.net), 2020-
