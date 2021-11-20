from table.results import Results

import unittest
from collections import namedtuple
from textwrap import dedent


class TestResults(unittest.TestCase):
    def test_no_rows(self):
        results = Results([])
        expected = "No results"
        actual = repr(results)
        self.assertEqual(expected, actual)

    def test_rows_and_cols(self):
        row = namedtuple("Row", ["foo", "bar"])
        rows = [
            row("Hi", "Bye"),
            row("Hello", "Goodbye!"),
        ]
        results = Results(rows)

        expected = dedent(
            """\
            +-------+----------+
            |  foo  |   bar    |
            +-------+----------+
            |    Hi |      Bye |
            | Hello | Goodbye! |
            +-------+----------+
        """
        ).strip()
        actual = repr(results)
        self.assertEqual(expected, actual)
