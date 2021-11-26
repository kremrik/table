from table.table import table as table_
from table.errors import TableError

import unittest
from dataclasses import dataclass
from datetime import date, datetime
from os import remove
from os.path import exists


class TestTable(unittest.TestCase):
    def test_dclass_not_dataclass(self):
        dclass = "WRONG"
        with self.assertRaises(TableError):
            table_(dclass, ":memory:")


class TestInMemoryTable(unittest.TestCase):
    def test_simple(self):
        @dataclass
        class Foo:
            name: str
            age: int

        table = table_(Foo)

        record = Foo("Joe", 30)
        table.insert(record)

        expected = [("Joe", 30)]
        actual = table.query("select * from foo")
        self.assertEqual(actual.rows, expected)

    def test_insert_many(self):
        @dataclass
        class Foo:
            name: str
            age: int

        table = table_(Foo)

        record1 = Foo("Joe", 30)
        record2 = Foo("Bill", 40)
        records = [record1, record2]
        table.insert(records)

        expected = [("Joe", 30), ("Bill", 40)]
        actual = table.query("select * from foo")
        self.assertEqual(actual.rows, expected)

    def test_filter_query(self):
        @dataclass
        class Foo:
            name: str
            age: int

        table = table_(Foo)

        record1 = Foo("Joe", 30)
        record2 = Foo("Bill", 40)
        records = [record1, record2]
        table.insert(records)

        expected = [("Bill", 40)]
        actual = table.query(
            "select * from foo where age > 30"
        )
        self.assertEqual(actual.rows, expected)

    def test_aggregate_query(self):
        @dataclass
        class Foo:
            name: str
            age: int

        table = table_(Foo)

        record1 = Foo("Joe", 30)
        record2 = Foo("Bill", 40)
        records = [record1, record2]
        table.insert(records)

        expected = [(70,)]
        actual = table.query(
            "select sum(age) as sum_age from foo"
        )
        self.assertEqual(actual.rows, expected)

    def test_date_datetime(self):
        @dataclass
        class Foo:
            dt: date
            dt_tm: datetime

        table = table_(Foo)

        dt = date.today()
        dt_tm = datetime.now()

        record = Foo(dt, dt_tm)
        table.insert(record)

        expected = [(dt, dt_tm)]
        actual = table.query("select * from foo")
        self.assertEqual(actual.rows, expected)

    def test_lowers_col_names(self):
        @dataclass
        class Foo:
            Name: str
            Age: int

        table = table_(Foo)

        record1 = Foo("Joe", 30)
        record2 = Foo("Bill", 40)
        records = [record1, record2]
        table.insert(records)

        expected = {
            "table": "foo",
            "columns": {
                "name": "TEXT",
                "age": "INTEGER",
            },
        }
        actual = table.schema
        self.assertEqual(actual, expected)


class TestPersistentTable(unittest.TestCase):
    TEST_DB = ".test_persistent_table.db"

    def setUp(self) -> None:
        try:
            remove(self.TEST_DB)
        except FileNotFoundError:
            pass

    def tearDown(self) -> None:
        self.setUp()

    def test_db_does_not_exist(self):
        @dataclass
        class Foo:
            name: str
            age: int

        table = table_(Foo, self.TEST_DB)

        record = Foo("Joe", 30)
        table.insert(record)

        expected = [("Joe", 30)]
        actual = table.query("select * from foo")

        with self.subTest():
            self.assertEqual(actual.rows, expected)
        with self.subTest():
            self.assertTrue(exists(self.TEST_DB))

    def test_db_and_table_exist(self):
        @dataclass
        class Foo:
            name: str
            age: int

        table = table_(Foo, self.TEST_DB)
        record = Foo("Joe", 30)
        table.insert(record)
        del table

        table2 = table_(Foo, self.TEST_DB)
        expected = [("Joe", 30)]
        actual = table2.query("select * from foo")

        self.assertEqual(actual.rows, expected)

    def test_db_exists_given_wrong_schema(self):
        @dataclass
        class Foo:
            name: str
            age: int

        table = table_(Foo, self.TEST_DB)
        del table

        @dataclass
        class Bar:
            height: int
            weight: int

        with self.assertRaises(TableError):
            table_(Bar, self.TEST_DB)

    def test_save_in_mem_to_persistent(self):
        @dataclass
        class Foo:
            name: str
            age: int

        table = table_(Foo)

        record = Foo("Joe", 30)
        table.insert(record)
        table.save(self.TEST_DB)
        del table

        table = table_(Foo, self.TEST_DB)
        expected = [("Joe", 30)]
        actual = table.query("select * from foo")

        with self.subTest():
            self.assertEqual(actual.rows, expected)
        with self.subTest():
            self.assertTrue(exists(self.TEST_DB))
