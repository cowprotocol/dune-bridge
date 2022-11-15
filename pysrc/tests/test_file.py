import os
import unittest

from pysrc.file import OUT_DIR, FileIO

TEST_FILE = "test.json"


def cleanup_files(func):
    def wrapped_func(self):
        func(self)
        try:
            os.remove(os.path.join(OUT_DIR, TEST_FILE))
        except FileNotFoundError:
            pass

    return wrapped_func


class MyTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.dune_records = [
            {"col1": "value01", "col2": "value02"},
            {"col1": "value11", "col2": "value12"},
        ]

    @cleanup_files
    def test_write_read_json(self):
        file_manager = FileIO()
        file_manager.write(self.dune_records, TEST_FILE)
        self.assertEqual(file_manager.read(TEST_FILE), self.dune_records)

    def test_skip_empty_write(self):
        file_io = FileIO()
        with self.assertLogs():
            file_io.write([], TEST_FILE)
        with self.assertRaises(FileNotFoundError):
            file_io.read(TEST_FILE)
