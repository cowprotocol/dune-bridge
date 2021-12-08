import time
import unittest

from ..utils import ensure_that_download_is_recent, dune_address


class MyTestCase(unittest.TestCase):
    def test_ensure_download_is_recent(self):
        now = int(time.time()) - 100
        with self.assertRaises(SystemExit):
            ensure_that_download_is_recent(now, 50)

        self.assertEqual(ensure_that_download_is_recent(now, 100), None)

    def test_dune_address(self):
        hex_address = "0xca8e1b4e6846bdd9c59befb38a036cfbaa5f3737"
        self.assertEqual(
            dune_address(hex_address),
            "\\xca8e1b4e6846bdd9c59befb38a036cfbaa5f3737"
        )



if __name__ == '__main__':
    unittest.main()
