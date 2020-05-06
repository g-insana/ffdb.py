# ffdb.py - Flat-File DB: nosql single file database and document storage

The module `ffdb` and the associated set of utility scripts
(`indexer`, `extractor`, `filestorer`, `remover` and `merger`)
allow the creation, maintenance and usage of a database and document storage
which employs a single file acting as a container for all the data.

This file can be distributed in several copies, made locally available or placed online
to be accessed remotely (e.g. over ftp or www).

The resulting database is hence accessible everywhere, without the need for any complex
installation and in particular without requiring any service to be running continuously.

* Simple 1-2-3 procedure:
1. Index your flat-file and/or choose documents to store
2. Place the resulting file wherever you want (locally or online, e.g. ftp or www)
3. Retrieve the entries or the documents from any device and location

**FFDB** can index entries according to any pattern, creating index of identifiers unique to each entry or shared across many. For a biological database this could mean for example to allow retrieval of all entries belonging to a certain species.

When requesting entries, the user can specify whether retrieving all the entries corresponding to the given identifier, or only the first or only the last one. This allows the possibility of storing a version history of the entries, continuously appending new entry versions to the file.

When indexing entries of a flat-file or storing documents inside the database,
the user can optionally specify to encrypt (AES) or compress (ZLIB) the entries/files.

Encrypted entries share a single master password but each has a unique IV ([Initialization vector](https://en.wikipedia.org/wiki/Initialization_vector)). No plaintext information is sent or received, thus allowing secure access to encrypted entries of the database over insecure channels.

With an external utility ([gztool](https://github.com/circulosmeos/gztool)), the
container file can even be compressed as a whole, while still allowing retrieval of
single entries. This can be useful when said file needs to also be
distributed and employed in its entirety, without limiting its use only via `ffdb`.

## Documentation

* [GitHub page](https://github.com/g-insana/ffdb.py/)

## Download and installation

`ffdb` is pure python code. It has no platform-specific dependencies and should thus work on all platforms. It requires the packages `requests` `pycryptodomex` and `sortedcontainers`.
The latest version of `ffdb` can be installed by typing either:

``` bash
pip3 install ffdb
```
  (from [Python Package Index](https://pypi.org/project/ffdb/))

or:
``` bash
pip3 install git+git://github.com/g-insana/ffdb.py.git
```
  (from [GitHub](https://github.com/g-insana/ffdb.py/)).

The utility scripts should get installed for you by pip.
Alternatively, you can directly download those you need:

``` bash
curl -LO https://github.com/g-insana/ffdb.py/raw/master/scripts/indexer.py
curl -LO https://github.com/g-insana/ffdb.py/raw/master/scripts/extractor.py
curl -LO https://github.com/g-insana/ffdb.py/raw/master/scripts/remover.py
curl -LO https://github.com/g-insana/ffdb.py/raw/master/scripts/merger.py
```

## Quickstart: some examples

``` bash
 #index a flat-file:

$ indexer.py -i '^name: (.+)$' 'email: (.+)$' -f addressbook >addressbook.idx

$ indexer.py -i '^AC   (.+?);' -f uniprot.dat -e '^//$' >uniprot.pac

 #store all the files contained in a directory:

$ filestorer.py -f myphotos.db -d iceland_photos/

 #add some more files

$ filestorer.py -f myphotos.db -s sunset.jpg icecream.jpg

 #extract a series of entries from the db

$ extractor.py -f uniprot.dat -i uniprot.pac -l sars_proteins

 #extract an entry from remote location

$ extractor.py -f http://remote.host/addressbook -i addressbook.idx -s john@abc.de

 #extract a file from the db

$ extractor.py -f myphotos.db -s sunset.jpg >sunset.jpg

 #merge two indexed flat-files

$ merger.py -f mydb -i mydb.idx -e newentries -n newentries.idx -d #(mydb will incorporate newentries)

 #remove a series of entries from the db

$ remover.py -f addressbook -i addressbook.idx -l disagreed_with_me.list
```

## Copyright

`ffdb` is licensed under the [GNU Affero General Public License](https://choosealicense.com/licenses/agpl-3.0/).

(c) Copyright [Giuseppe Insana](http://insana.net), 2020-
