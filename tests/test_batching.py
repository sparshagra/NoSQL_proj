import os
import tempfile
import unittest

from etl import batching as b


class BatchRule(unittest.TestCase):
    def test_batch_id_boundaries(self):
        self.assertEqual(b.batch_id_for_line(1, 100), 1)
        self.assertEqual(b.batch_id_for_line(100, 100), 1)
        self.assertEqual(b.batch_id_for_line(101, 100), 2)
        self.assertEqual(b.batch_id_for_line(250, 100), 3)

    def test_matches_ceil_definition(self):
        import math
        for size in (1, 7, 100, 1000):
            for n in range(1, 2500, 37):
                self.assertEqual(b.batch_id_for_line(n, size), math.ceil(n / size))

    def test_batch_count_and_average(self):
        self.assertEqual(b.expected_batch_count(250, 100), 3)
        self.assertEqual(b.expected_batch_count(300, 100), 3)
        self.assertEqual(b.expected_batch_count(0, 100), 0)
        self.assertAlmostEqual(b.average_batch_size(250, 100), 250 / 3)
        with self.assertRaises(ValueError):
            b.average_batch_size(0, 100)

    def test_invalid_inputs(self):
        for bad in (0, -1, 1.5, True, "10"):
            with self.assertRaises(ValueError):
                b.validate_batch_size(bad)
        with self.assertRaises(ValueError):
            b.batch_id_for_line(0, 10)


class LineIteration(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.dir.cleanup)

    def _write(self, name, data: bytes) -> str:
        p = os.path.join(self.dir.name, name)
        with open(p, "wb") as fh:
            fh.write(data)
        return p

    def test_global_numbering_across_files_counts_blank_and_unterminated(self):
        f1 = self._write("a", b"l1\n\nl3\r\n")          # blank line and CRLF count
        f2 = self._write("b", b"l4\nl5")                 # no trailing newline
        lines = list(b.iter_numbered_lines([f1, f2]))
        self.assertEqual(lines, [(1, "l1"), (2, ""), (3, "l3"), (4, "l4"), (5, "l5")])
        self.assertEqual(b.count_lines([f1, f2]), 5)

    def test_count_matches_iteration(self):
        f = self._write("c", b"x\n" * 1234 + b"y")
        self.assertEqual(b.count_lines([f]), sum(1 for _ in b.iter_numbered_lines([f])))

    def test_non_utf8_bytes_do_not_crash(self):
        f = self._write("d", b"caf\xe9 \xff\xfe\n")
        self.assertEqual(len(list(b.iter_numbered_lines([f]))), 1)

    def test_iter_batches(self):
        f1 = self._write("a", b"1\n2\n3\n")
        f2 = self._write("b", b"4\n5\n")
        got = [(bid, [n for n, _ in lines]) for bid, lines in b.iter_batches([f1, f2], 2)]
        self.assertEqual(got, [(1, [1, 2]), (2, [3, 4]), (3, [5])])   # batch 2 spans both files


if __name__ == "__main__":
    unittest.main()
