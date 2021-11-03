import time
import unittest

from ..utils import ensure_that_download_is_recent


class MyTestCase(unittest.TestCase):
    def test_ensure_download_is_recent(self):
        now = int(time.time()) - 100
        with self.assertRaises(SystemExit):
            ensure_that_download_is_recent(now, 50)

        self.assertEqual(ensure_that_download_is_recent(now, 100), None)


if __name__ == '__main__':
    unittest.main()
