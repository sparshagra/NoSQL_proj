"""Tests for etl/reference_parser.py (Member B), i.e. for docs/contract.md.

Real lines are copied from the NASA logs (global line number in the comment). Lines marked
SYNTHETIC do not occur in the data; they test rules the real data never exercises
(blank lines, other time-zone offsets, broken structure).
"""

import os
import re
import tempfile
import unittest

from etl import reference_parser as rp
from etl.batching import count_lines
from etl.contract import check_output, find_problems


def write(path, data: bytes):
    with open(path, "wb") as fh:
        fh.write(data)


def log_line(host="h1", ts="01/Jul/1995:00:00:01 -0400", req="GET /a HTTP/1.0", status=200, size="10"):
    """SYNTHETIC line builder for query tests."""
    return f'{host} - - [{ts}] "{req}" {status} {size}'


class OuterStructure(unittest.TestCase):
    def test_ordinary_line(self):  # [1]
        r = rp.parse_line('199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245')
        self.assertEqual(r, {
            "host": "199.72.81.55", "timestamp": "01/Jul/1995:00:00:01 -0400",
            "log_date": "1995-07-01", "log_hour": 0, "http_method": "GET",
            "resource_path": "/history/apollo/", "protocol_version": "HTTP/1.0",
            "status_code": 200, "bytes_transferred": 6245,
        })
        self.assertEqual(tuple(r), rp.FIELDS)

    def test_dash_bytes_become_zero(self):  # [51]
        r = rp.parse_line('dd15-062.compuserve.com - - [01/Jul/1995:00:01:12 -0400] '
                          '"GET /news/sci.space.shuttle/archive/sci-space-shuttle-22-apr-1995-40.txt HTTP/1.0" 404 -')
        self.assertEqual((r["status_code"], r["bytes_transferred"]), (404, 0))

    def test_explicit_zero_bytes(self):  # [7]
        r = rp.parse_line('burger.letters.com - - [01/Jul/1995:00:00:12 -0400] '
                          '"GET /shuttle/countdown/video/livevideo.gif HTTP/1.0" 200 0')
        self.assertEqual(r["bytes_transferred"], 0)

    def test_truncated_last_jul_line_is_malformed(self):  # [1891715]
        self.assertIsNone(rp.parse_line("alyssa.p"))

    def test_blank_and_space_lines_are_malformed(self):  # SYNTHETIC
        self.assertIsNone(rp.parse_line(""))
        self.assertIsNone(rp.parse_line("   "))

    def test_broken_structure_is_malformed(self):  # SYNTHETIC
        good = log_line()
        self.assertIsNotNone(rp.parse_line(good))
        for bad in (
            good.replace(" 200 ", " 2000 "),             # status not 3 digits
            good.replace(" 200 10", " 200 1x"),          # bytes not digits or '-'
            good.replace(" 200 10", " 200"),             # bytes missing
            good.replace("Jul", "Foo"),                  # unknown month
            good.replace('"GET /a HTTP/1.0"', "GET /a HTTP/1.0"),  # request not quoted
            good + " ",                                  # trailing space: full match required
            good.replace("h1 ", "h1  "),                 # double space after host
        ):
            with self.subTest(bad=bad):
                self.assertIsNone(rp.parse_line(bad))

    def test_no_timezone_conversion(self):  # SYNTHETIC offsets; date/hour are the digits as written
        r = rp.parse_line(log_line(ts="31/Aug/1995:23:59:53 +0530"))
        self.assertEqual((r["log_date"], r["log_hour"]), ("1995-08-31", 23))
        r = rp.parse_line(log_line(ts="01/Jul/1995:00:00:01 -0400"))
        self.assertEqual((r["log_date"], r["log_hour"]), ("1995-07-01", 0))
        self.assertEqual(r["timestamp"], "01/Jul/1995:00:00:01 -0400")

    def test_every_month(self):  # SYNTHETIC
        for i, mon in enumerate("Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec".split(), start=1):
            self.assertEqual(rp.parse_line(log_line(ts=f"05/{mon}/1995:07:00:00 -0400"))["log_date"],
                             f"1995-{i:02d}-05")

    def test_non_ascii_digits_are_not_digits(self):  # SYNTHETIC: Python \d would accept these, Java would not
        self.assertIsNone(rp.parse_line(log_line(status="２００")))

    def test_regexes_use_only_portable_classes(self):
        for rx in (rp.LINE_REGEX, rp.REQUEST_WITH_PROTOCOL_REGEX, rp.REQUEST_WITHOUT_PROTOCOL_REGEX):
            self.assertIsNone(re.search(r"\\[dDsSwW]", rx), rx)
            self.assertTrue(rx.startswith("(?s)^") and rx.endswith("$"), rx)


class RequestSplit(unittest.TestCase):
    CASES = [
        # (request text, expected (method, path, protocol)); [n] = real global line number
        ("GET /history/apollo/ HTTP/1.0", ("GET", "/history/apollo/", "HTTP/1.0")),                 # [1]
        ("GET /shuttle/missions/sts-71/movies/sts-71-mir-dock.mpg",
         ("GET", "/shuttle/missions/sts-71/movies/sts-71-mir-dock.mpg", None)),                      # [1286] no protocol
        ("GET /shuttle/missions/sts-67/images/images.html   HTTP/1.0",
         ("GET", "/shuttle/missions/sts-67/images/images.html", "HTTP/1.0")),                        # [9001] extra spaces
        ("GET /finger @net.com HTTP/1.0", ("GET", "/finger @net.com", "HTTP/1.0")),                 # [22545] inner space
        ("GET /htbin/wais.pl?51-L HTTP/1.0", ("GET", "/htbin/wais.pl?51-L", "HTTP/1.0")),           # [811] query string
        ('GET /images/" HTTP/1.0', ("GET", '/images/"', "HTTP/1.0")),                                # [131906] embedded quote
        ('GET /shuttle/missions/sts-57/sts-57-patch-small.gif"" HTTP/1.0',
         ("GET", '/shuttle/missions/sts-57/sts-57-patch-small.gif""', "HTTP/1.0")),                  # [2894958]
        ("GET /history/history.html HTTP/V1.0", ("GET", "/history/history.html", "HTTP/V1.0")),     # HTTP/V1.0
        ("GET /ksc.html HTTP/*", ("GET", "/ksc.html", "HTTP/*")),                                   # [1348951]
        ("GET  HTTP/1.0", ("GET", None, "HTTP/1.0")),                                               # [727224] no path
        ("\x05\x01", ("\x05\x01", None, None)),                                                      # [691503] binary
        ("1/history/apollo/images/", ("1/history/apollo/images/", None, None)),                      # [1421675]
        ("GET /htbin/cdt_clock.pl HTTP/1.0From:  <berend@blazemonger.pc.cc.cmu.edu>",
         ("GET", "/htbin/cdt_clock.pl HTTP/1.0From:  <berend@blazemonger.pc.cc.cmu.edu>", None)),   # [775596]
        ("GET /shuttle/resources/orbiters/\x86b\xd7\t\x0c HTTP/1.0",
         ("GET", "/shuttle/resources/orbiters/\x86b\xd7\t\x0c", "HTTP/1.0")),                        # [551066] tab, non-UTF-8
        ("GET /icons/back.xbm ", ("GET", "/icons/back.xbm", None)),                                  # [25571] trailing space
        # SYNTHETIC
        ("", (None, None, None)),
        ("   ", (None, None, None)),
        ("GET", ("GET", None, None)),
        ("HTTP/1.0", ("HTTP/1.0", None, None)),
        ("GET /a HTTP/1.0 HTTP/1.1", ("GET", "/a HTTP/1.0", "HTTP/1.1")),
        ("get /a http/1.0", ("get", "/a http/1.0", None)),          # protocol prefix is case-sensitive
        ("GET\t/a HTTP/1.0", ("GET\t/a", None, "HTTP/1.0")),        # only U+0020 separates tokens
    ]

    def test_cases(self):
        for req, expected in self.CASES:
            with self.subTest(req=req):
                self.assertEqual(rp.parse_request(req), expected)

    def test_regex_matches_word_definition(self):
        """docs/contract.md 4.1 states the rule twice (regexes and words); they must agree."""
        def by_words(req):
            s = req.strip(" ")
            if not s:
                return None, None, None
            toks = [t for t in s.split(" ") if t]
            method, rest = toks[0], s[len(toks[0]):]
            if len(toks) >= 2 and toks[-1].startswith("HTTP/"):
                body = rest.rstrip(" ")[: -len(toks[-1])]
                return method, body.strip(" ") or None, toks[-1]
            return method, rest.strip(" ") or None, None

        for req, _ in self.CASES:
            with self.subTest(req=req):
                self.assertEqual(rp.parse_request(req), by_words(req))


class NumberingAndFiles(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def path(self, name):
        return os.path.join(self.tmp.name, name)

    def test_file_without_final_newline_does_not_merge_with_next_file(self):
        jul, aug = self.path("jul"), self.path("aug")
        write(jul, (log_line() + "\n").encode() + b"alyssa.p")            # like the real Jul95 end
        write(aug, (log_line(ts="01/Aug/1995:00:00:01 -0400") + "\n").encode())
        recs = list(rp.iter_records([jul, aug], batch_size=2))
        self.assertEqual([r["line_number"] for r in recs], [1, 2, 3])
        self.assertEqual([r["malformed"] for r in recs], [False, True, False])
        self.assertEqual([r["batch_id"] for r in recs], [1, 1, 2])
        self.assertEqual(recs[2]["log_date"], "1995-08-01")

    def test_blank_line_counts_and_is_malformed(self):  # SYNTHETIC
        f = self.path("f")
        write(f, (log_line() + "\n\n" + log_line() + "\n").encode())
        recs = list(rp.iter_records([f], batch_size=1))
        self.assertEqual([(r["line_number"], r["batch_id"], r["malformed"]) for r in recs],
                         [(1, 1, False), (2, 2, True), (3, 3, False)])

    def test_trailing_newline_is_not_a_blank_record(self):  # SYNTHETIC; confirmed decision: blank line = malformed
        cases = [
            (log_line().encode() + b"\n",     [False]),          # final \n ends line 1; no extra record
            (log_line().encode(),             [False]),          # no final \n: same single record
            (log_line().encode() + b"\n\n",   [False, True]),    # the 2nd \n ends a real blank line 2
            (b"\n",                           [True]),           # one blank line
            (b"\n\n\n",                       [True, True, True]),
            (b"",                             []),               # empty file: no records
        ]
        for data, malformed in cases:
            with self.subTest(data=data):
                f = self.path("f")
                write(f, data)
                recs = list(rp.iter_records([f], batch_size=10))
                self.assertEqual([r["malformed"] for r in recs], malformed)
                self.assertEqual(count_lines([f]), len(malformed))   # agrees with A's line count

    def test_crlf_is_stripped_and_lone_cr_is_data(self):  # SYNTHETIC (real files have neither)
        f = self.path("f")
        write(f, (log_line() + "\r\n").encode() + b"a\rb\n")
        recs = list(rp.iter_records([f], batch_size=10))
        self.assertEqual(len(recs), 2)            # lone \r does not end a line
        self.assertFalse(recs[0]["malformed"])
        self.assertTrue(recs[1]["malformed"])

    def test_final_line_without_newline_is_counted(self):
        f = self.path("f")
        write(f, (log_line() + "\n" + log_line(status=404)).encode())
        recs = list(rp.iter_records([f], batch_size=10))
        self.assertEqual([(r["line_number"], r["malformed"]) for r in recs], [(1, False), (2, False)])
        self.assertEqual(recs[1]["status_code"], 404)

    def test_latin1_bytes_round_trip(self):  # [551066] tab and non-UTF-8 bytes inside the path
        raw = (b'p12.haifa1.actcom.co.il - - [07/Jul/1995:17:49:26 -0400] '
               b'"GET /shuttle/resources/orbiters/\x86b\xd7\t\x0c HTTP/1.0" 404 -\n')
        f = self.path("f")
        write(f, raw)
        (rec,) = rp.iter_records([f], batch_size=1)
        self.assertFalse(rec["malformed"])
        self.assertEqual(rec["resource_path"].encode("latin-1"), b"/shuttle/resources/orbiters/\x86b\xd7\t\x0c")

    def test_batch_boundaries(self):
        f = self.path("f")
        write(f, "".join(log_line() + "\n" for _ in range(7)).encode())
        recs = list(rp.iter_records([f], batch_size=3))
        self.assertEqual([r["batch_id"] for r in recs], [1, 1, 1, 2, 2, 2, 3])
        out = rp.reference_output([f], 3)
        self.assertEqual((out["total_records"], out["batch_count"]), (7, 3))
        self.assertTrue(all(r["batch_id"] == 3 for r in out["rows"]))     # decision D1 (A approved; instructor pending)


class Queries(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def run_lines(self, lines, batch_size=1000):
        f = os.path.join(self.tmp.name, "log")
        write(f, ("\n".join(lines) + "\n").encode("latin-1"))
        out = rp.reference_output([f], batch_size)
        check_output(out, batch_size)            # A's checker accepts the reference output
        return out

    @staticmethod
    def rows(out, qid):
        return [r for r in out["rows"] if r["query_id"] == qid]

    def test_q1(self):
        out = self.run_lines([
            log_line(status=200, size="10"), log_line(status=200, size="-"),
            log_line(status=404, size="-"), log_line(ts="02/Jul/1995:05:00:00 -0400", status=200, size="5"),
            "garbage",
        ])
        q1 = {(r["log_date"], r["status_code"]): (r["request_count"], r["total_bytes"]) for r in self.rows(out, "Q1")}
        self.assertEqual(q1, {("1995-07-01", 200): (2, 10), ("1995-07-01", 404): (1, 0), ("1995-07-02", 200): (1, 5)})
        self.assertEqual((out["total_records"], out["malformed_count"]), (5, 1))
        self.assertEqual(set(self.rows(out, "Q1")[0]) - {"query_id", "pipeline", "run_id", "batch_id", "executed_at"},
                         {"log_date", "status_code", "request_count", "total_bytes"})

    def test_q2_top20_tie_break_and_null_path(self):
        lines = []
        for i in range(25):                      # /p00 .. /p24, each requested twice
            lines += [log_line(host="a", req=f"GET /p{i:02d} HTTP/1.0")] * 2
        lines += [log_line(host="b", req="GET /p24 HTTP/1.0")]          # /p24 -> 3 requests, 2 hosts
        lines += [log_line(req="GET  HTTP/1.0")] * 5                    # NULL path: excluded from Q2
        out = self.run_lines(lines)
        q2 = self.rows(out, "Q2")
        self.assertEqual(len(q2), 20)
        self.assertEqual(q2[0]["resource_path"], "/p24")
        self.assertEqual((q2[0]["request_count"], q2[0]["distinct_host_count"]), (3, 2))
        # remaining 19 places: ties at 2 requests, cut by resource_path ascending -> /p00../p18
        self.assertEqual([r["resource_path"] for r in q2[1:]], [f"/p{i:02d}" for i in range(19)])
        self.assertNotIn(None, [r["resource_path"] for r in q2])

    def test_q2_order_is_code_point_order(self):
        out = self.run_lines([log_line(req=f"GET {p} HTTP/1.0") for p in ("/b", "/B", "/a", "/_")])
        self.assertEqual([r["resource_path"] for r in self.rows(out, "Q2")], ["/B", "/_", "/a", "/b"])

    def test_q3(self):
        out = self.run_lines([
            log_line(host="x", status=404), log_line(host="x", status=500), log_line(host="y", status=400),
            log_line(host="z", status=200), log_line(host="z", status=399), log_line(host="z", status=600),
            log_line(ts="01/Jul/1995:01:00:00 -0400", status=200),        # hour with no errors
        ])
        q3 = {(r["log_date"], r["log_hour"]): r for r in self.rows(out, "Q3")}
        h0, h1 = q3[("1995-07-01", 0)], q3[("1995-07-01", 1)]
        self.assertEqual((h0["error_request_count"], h0["total_request_count"], h0["distinct_error_hosts"]), (3, 6, 2))
        self.assertEqual(h0["error_rate"], 3 / 6)                         # fraction, not percent
        self.assertEqual((h1["error_request_count"], h1["total_request_count"],
                          h1["error_rate"], h1["distinct_error_hosts"]), (0, 1, 0.0, 0))

    def test_error_rate_not_rounded(self):
        out = self.run_lines([log_line(status=404)] + [log_line(status=200)] * 2)
        self.assertEqual(self.rows(out, "Q3")[0]["error_rate"], 1 / 3)

    def test_blank_lines_count_in_totals_and_batches(self):  # SYNTHETIC; confirmed decision: blank = malformed
        lines = [log_line(), "", log_line(status=404), "   ", "", log_line(ts="01/Jul/1995:01:00:00 -0400")]
        out = self.run_lines(lines, batch_size=4)            # run_lines also runs A's checker
        self.assertEqual((out["total_records"], out["malformed_count"], out["batch_count"]), (6, 3, 2))
        self.assertEqual(sum(r["request_count"] for r in self.rows(out, "Q1")), 3)
        self.assertEqual(sum(r["total_request_count"] for r in self.rows(out, "Q3")), 3)
        f = os.path.join(self.tmp.name, "log")
        recs = list(rp.iter_records([f], batch_size=4))
        self.assertEqual([(r["line_number"], r["batch_id"], r["malformed"]) for r in recs],
                         [(1, 1, False), (2, 1, True), (3, 1, False), (4, 1, True), (5, 2, True), (6, 2, False)])

    def test_totals_invariant_with_odd_requests(self):
        out = self.run_lines([log_line(req="\x05\x01", status=400), log_line(req="", status=400),
                              log_line(req="GET /x"), "", "garbage"])
        self.assertEqual(find_problems(out, 1000), [])
        self.assertEqual(out["malformed_count"], 2)
        self.assertEqual(self.rows(out, "Q3")[0]["error_request_count"], 2)   # odd requests still count


if __name__ == "__main__":
    unittest.main()
