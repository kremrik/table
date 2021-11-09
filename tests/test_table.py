from table.table import Table

import unittest
from dataclasses import dataclass


class TestTable(unittest.TestCase):
    def test_simple(self):
        @dataclass
        class Foo:
            name: str
            age: int
        
        table = Table(Foo)

        record = Foo("Joe", 30)
        table.insert(record)

        expected = [("Joe", 30)]
        actual = table.query("select * from foo")
        self.assertEqual(actual, expected)

    def test_insert_many(self):
        @dataclass
        class Foo:
            name: str
            age: int
        
        table = Table(Foo)

        record1 = Foo("Joe", 30)
        record2 = Foo("Bill", 40)
        records = [record1, record2]
        table.insert(records)

        expected = [("Joe", 30), ("Bill", 40)]
        actual = table.query("select * from foo")
        self.assertEqual(actual, expected)

    def test_filter_query(self):
        @dataclass
        class Foo:
            name: str
            age: int
        
        table = Table(Foo)

        record1 = Foo("Joe", 30)
        record2 = Foo("Bill", 40)
        records = [record1, record2]
        table.insert(records)

        expected = [("Bill", 40)]
        actual = table.query("select * from foo where age > 30")
        self.assertEqual(actual, expected)

    def test_aggregate_query(self):
        @dataclass
        class Foo:
            name: str
            age: int
        
        table = Table(Foo)

        record1 = Foo("Joe", 30)
        record2 = Foo("Bill", 40)
        records = [record1, record2]
        table.insert(records)

        expected = [(70,)]
        actual = table.query("select sum(age) as sum_age from foo")
        self.assertEqual(actual, expected)
