import os
import tempfile
import unittest

from scripts.check_line_endings import scan


class LineEndingScan(unittest.TestCase):
    def _scan(self, data: bytes) -> dict:
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "f")
            with open(p, "wb") as fh:
                fh.write(data)
            return scan(p)

    def test_plain_lf(self):
        self.assertEqual(self._scan(b"a\nb\n"), {"crlf": 0, "lone_cr": 0})

    def test_crlf_and_lone_cr(self):
        self.assertEqual(self._scan(b"a\r\nb\rc\r\n"), {"crlf": 2, "lone_cr": 1})

    def test_cr_at_eof(self):
        self.assertEqual(self._scan(b"a\nb\r"), {"crlf": 0, "lone_cr": 1})

    def test_crlf_split_across_chunk_boundary(self):
        data = b"x" * ((1 << 20) - 1) + b"\r\n" + b"y\n"      # \r is the last byte of the first 1 MiB chunk
        self.assertEqual(self._scan(data), {"crlf": 1, "lone_cr": 0})

    def test_lone_cr_at_chunk_boundary(self):
        data = b"x" * ((1 << 20) - 1) + b"\r" + b"y\n"
        self.assertEqual(self._scan(data), {"crlf": 0, "lone_cr": 1})


if __name__ == "__main__":
    unittest.main()
