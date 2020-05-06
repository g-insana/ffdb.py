#!/usr/bin/env python3
"""
tests for ffdb
simply run 'pytest' to run all tests
"""
import ffdb
import pytest
from subprocess import Popen, PIPE, DEVNULL

data = """
Sed ut perspiciatis unde omnis iste natus error sit voluptatem accusantium
doloremque laudantium, totam rem aperiam, eaque ipsa quae ab illo inventore
veritatis et quasi architecto beatae vitae dicta sunt explicabo.
Nemo enim ipsam voluptatem quia voluptas sit aspernatur aut odit aut fugit,
sed quia consequuntur magni dolores eos qui ratione voluptatem sequi nesciunt.
Neque porro quisquam est, qui dolorem ipsum quia dolor sit amet, consectetur,
adipisci velit, sed quia non numquam eius modi tempora incidunt ut labore et
dolore magnam aliquam quaerat voluptatem. Ut enim ad minima veniam,
quis nostrum exercitationem ullam corporis suscipit laboriosam, nisi ut aliquid
ex ea commodi consequatur? Quis autem vel eum iure reprehenderit qui in ea
voluptate velit esse quam nihil molestiae consequatur, vel illum qui dolorem
eum fugiat quo voluptas nulla pariatur?
"""

entry = dict()
entry['full'] = data
entry['length_b64'] = ffdb.int_to_b64(len(data))
entry['position'] = 0
entry['ids'] = ['lorem', 'ipsum', 'lorem ipsum']
entry['iv'] = b'N\x8e\xd8\xae\x08\x0c\x18\xab\xf4_\x93\x87\x95\x01\x01X'

testfile = 'tests/dogtest.dat' #8 UniProt entries of Canis lupus familiaris
testindex_ac = 'tests/dogtest.ac.idx'
testlist_ac = 'tests/dogtest.ac'


@pytest.fixture(scope="session")
def tmp_test_dir(tmpdir_factory):
    temporary_test_dir = tmpdir_factory.mktemp("ffdb")
    return temporary_test_dir


class TestIndexFormats:
    def test_format_indexes(self):
        """
        Check for correct parsing of indexes
        """
        cipher_type = "A"
        for try_index_type in ("-", ".", ":", "+"):
            new_indexes = ffdb.format_indexes(entry, try_index_type,
                                              cipher_type, checksums=False)
            assert len(new_indexes) == len(entry['ids'])

            firstline = new_indexes[0].rstrip()
            ffdb.eprint("index_type: '{}', index_line: '{}'".format(try_index_type, firstline))

            index_type, cipher_name, keysize, _ = ffdb.check_index(firstline)

            assert try_index_type == index_type
            if cipher_name is not None:
                assert cipher_type == ffdb.get_cipher_type(cipher_name)


class TestEntryProcessing:
    """
    In this group are tests for correctness of data processing:
        compression/decompression
        encryption/decryption
    """
    def test_deflate(self):
        bytestring = data.encode('UTF-8')
        deflated = ffdb.deflate(bytestring, 9)
        inflated = ffdb.inflate(deflated).decode('UTF-8')
        ffdb.eprint("testing inflate/deflate of data")
        assert inflated == data, "problems with compression or uncompression"

    def test_ciphers(self):
        global SALT
        SALT = b'5ed3a4284d6a9c1e4e4f6b4729b254be'
        passphrase = "The quick brown fox jumps over the lazy dog"
        iv = ffdb.generate_iv()

        ffdb.eprint("Testing aes128")
        keysize = 16
        cipher_name, key = ffdb.derive_key(passphrase, keysize)
        cipher_type = ffdb.get_cipher_type(cipher_name)
        assert cipher_name == "aes128"
        assert key == b'c\x05i\xa5\x81c`\x8e(\xa4\xd3CR\xc9\xb0\xf1'
        assert cipher_type == "A"

        ffdb.eprint("Testing aes192")
        keysize = 24
        cipher_name, key = ffdb.derive_key(passphrase, keysize)
        cipher_type = ffdb.get_cipher_type(cipher_name)
        assert cipher_name == "aes192"
        assert key == b'c\x05i\xa5\x81c`\x8e(\xa4\xd3CR\xc9\xb0\xf1\x86L\x7f=a\xd3\x8cw'
        assert cipher_type == "B"

        ffdb.eprint("Testing aes256")
        keysize = 32
        cipher_name, key = ffdb.derive_key(passphrase, keysize)
        cipher_type = ffdb.get_cipher_type(cipher_name)
        assert cipher_name == "aes256"
        assert key == b'c\x05i\xa5\x81c`\x8e(\xa4\xd3CR\xc9\xb0\xf1\x86L\x7f=a\xd3\x8cw\xadc:\x899\xfe^\xfe'
        assert cipher_type == "C"

        ffdb.eprint("Testing encryption/decryption")
        for keysize in (16, 24, 32):
            cipher_name, key = ffdb.derive_key(passphrase, keysize)

            cipher = ffdb.init_cipher(key, iv)
            encrypted_data = cipher.encrypt(data.encode('UTF-8'))
            compressed_encrypted_data = cipher.encrypt(ffdb.deflate(data.encode('UTF-8'), 9))

            cipher = ffdb.init_cipher(key, iv)
            decrypted_data = cipher.decrypt(encrypted_data).decode('UTF-8')
            decrypted_uncompressed_data = ffdb.inflate(cipher.decrypt(compressed_encrypted_data
                                                                      )).decode('UTF-8')
            assert decrypted_data == data
            assert decrypted_uncompressed_data == data


class TestFileIndexing:
    def test_datafiles_io(self, tmp_test_dir):
        """
        Check test datafiles are in place and readable/writeable
        """
        tmpwrite = tmp_test_dir.join("tmpwrite")
        ffdb.check_iofiles([testfile, testindex_ac], [tmpwrite])

    def test_entry_indexing(self):
        """
        Check for correct indexing of file
        """
        ffdb.check_iofiles([testfile, testindex_ac], [])
        indexer_call = ['indexer.py', '-i', '^AC   (.+?);', '-e', '^//$',
                        '-f', testfile, '-x']
        indexer_pipe = Popen(indexer_call, stdout=PIPE, stderr=DEVNULL)
        with open(testindex_ac, 'rb') as fh:
            for line1, line2 in zip(fh, indexer_pipe.stdout.readlines()):
                assert line1 == line2

    def test_entry_extraction(self):
        """
        Check for correct entry extraction
        """
        ffdb.check_iofiles([testfile, testindex_ac], [])
        extractor_call = ['extractor.py', '-f', testfile, '-i', testindex_ac,
                          '-l', testlist_ac, '-x']
        extractor_pipe = Popen(extractor_call, stdout=PIPE, stderr=DEVNULL)
        with open(testfile, 'rb') as fh:
            for line1, line2 in zip(fh, extractor_pipe.stdout.readlines()):
                assert line1 == line2
