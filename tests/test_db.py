from table.db import Converter, Database, DatabaseError

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
            db.executemany(stmt, records)

            expected = [("hi", 1), ("hello", 2)]
            actual = db.execute("SELECT * FROM test")

            self.assertEqual(actual, expected)

    @unittest.skip("v2 feature")
    def test_custom_type(self):
            db = Database()

            class Point:
                def __init__(self, x, y):
                    self.x, self.y = x, y
                def __eq__(self, o: object) -> bool:
                    return self.x == o.x and self.y == o.y
            
            def adapt_point(point):
                return ("%f;%f" % (point.x, point.y)).encode('ascii')

            def convert_point(s):
                x, y = list(map(float, s.split(b";")))
                return Point(x, y)

            tablename = "test"
            schema = {"foo": str, "bar": Point}
            converter = Converter(
                column="bar",
                type=Point,
                adapter=adapt_point,
                converter=convert_point,
            )

            db.create_table(tablename, schema, [converter])

            stmt = "INSERT INTO test (foo, bar) VALUES (?, ?)"
            records = [("hi", Point(1, 2)), ("hello", Point(3, 4))]
            db.executemany(stmt, records)

            expected = [("hi", Point(1, 2)), ("hello", Point(3, 4))]
            actual = db.execute("SELECT * FROM test")

            self.assertEqual(actual, expected)
