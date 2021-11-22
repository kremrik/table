from table.db import (
    Database,
    DatabaseError,
    DatabaseWarning,
)

import unittest


class TestDatabase(unittest.TestCase):
    def test_simple_table_create_insert_query(self):
        db = Database()

        tablename = "test"
        schema = {"foo": str, "bar": int}
        db.create_table(tablename, schema)

        stmt = "INSERT INTO test (foo, bar) VALUES (?, ?)"
        record = ("hi", 1)
        db.execute(stmt, record)

        expected = [("hi", 1)]
        actual = db.execute("SELECT * FROM test")

        self.assertEqual(actual, expected)

    def test_insert_multiple_records(self):
        db = Database()

        tablename = "test"
        schema = {"foo": str, "bar": int}
        db.create_table(tablename, schema)

        stmt = "INSERT INTO test (foo, bar) VALUES (?, ?)"
        records = [("hi", 1), ("hello", 2)]
        db.execute(stmt, records)

        expected = [("hi", 1), ("hello", 2)]
        actual = db.execute("SELECT * FROM test")

        self.assertEqual(actual, expected)

    def test_schema(self):
        db = Database()

        tablename = "test"
        schema = {"foo": str, "bar": int}
        db.create_table(tablename, schema)

        expected = [
            {
                "cid": 0,
                "name": "foo",
                "type": "TEXT",
                "notnull": 0,
                "dflt_value": None,
                "pk": 0,
            },
            {
                "cid": 1,
                "name": "bar",
                "type": "INTEGER",
                "notnull": 0,
                "dflt_value": None,
                "pk": 0,
            },
        ]
        actual = db.schema(tablename)
        self.assertEqual(actual, expected)

    def test_drop_table(self):
        db = Database()

        tablename = "test"
        schema = {"foo": str, "bar": int}
        db.create_table(tablename, schema)
        db.drop_table("test")

        with self.assertRaises(DatabaseError):
            db.execute("select * from test")

    def test_table_exists_warning(self):
        db = Database()
        tablename = "test"
        schema = {"foo": str, "bar": int}
        db.create_table(tablename, schema)

        with self.assertRaises(DatabaseWarning):
            tablename = "test"
            schema = {"foo": str, "bar": int}
            db.create_table(tablename, schema)

    def test_table_exists_and_given_wrong_schema(self):
        db = Database()
        tablename = "test"
        schema = {"foo": str, "bar": int}
        db.create_table(tablename, schema)

        with self.assertRaises(DatabaseError):
            tablename = "test"
            schema = {"foo": str, "baz": str}
            db.create_table(tablename, schema)

    def test_schema_with_bad_type(self):
        db = Database()
        tablename = "test"
        schema = {"foo": set, "bar": int}

        with self.assertRaises(DatabaseError):
            db.create_table(tablename, schema)

    def test_table_exists(self):
        db = Database()

        tablename = "test"
        schema = {"foo": str, "bar": int}

        self.assertFalse(db.table_exists(tablename))

        db.create_table(tablename, schema)
        self.assertTrue(db.table_exists(tablename))

    def test_index_column(self):
        db = Database()

        tablename = "test"
        schema = {"foo": str, "bar": int}
        db.create_table(tablename, schema)
        db.create_index("test", "foo")

        expected = "idx_test_foo"
        actual = db.execute(
            "select name from sqlite_master where type = 'index'"
        )
        self.assertEqual(expected, actual[0].name)
