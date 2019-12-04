import itertools
from io import BytesIO
import unittest

from druzhba.avro import write_avro_file


class TestS3AvroRs(unittest.TestCase):
    fields = [
        {'name': 'a', 'type': 'int'},
        {'name': 'b', 'type': 'string'}
    ]

    def test_write_avro_increment_full(self):
        data = itertools.repeat({'a': 1, 'b': 'foo'}, 10)

        with BytesIO() as f:
            out = write_avro_file(f, data, self.fields, 'tbl', 1024)

        # method should return true if we fully drain the iterator
        self.assertTrue(out)

        # confirm we read to the end of the iterator
        remaining = len([x for x in data])
        self.assertEqual(remaining, 0)

    def test_write_avro_increment_partial(self):
        data = itertools.repeat({'a': 1, 'b': 'foo'}, 10000)

        with BytesIO() as f:
            complete, nrows = write_avro_file(f, data, self.fields, 'tbl', 1024)

        remaining = len([x for x in data])

        # method should return false if there are elements remaining
        self.assertFalse(complete)

        # confirm we drained some elements from the iterator
        self.assertLess(remaining, 10000)

        # confirm we logged some rows as being consumed
        self.assertGreater(nrows, 0)

        # confirm total rows minus consumed rows equals the number remaining
        self.assertEqual(remaining, 10000 - nrows)

        # meta: confirm there are in fact element remaining
        self.assertGreater(remaining, 0)
