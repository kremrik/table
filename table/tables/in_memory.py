from table.db import Database
from table.tables.base import Table


__all__ = ["InMemoryTable"]


class InMemoryTable(Table):
    @staticmethod
    def _connect(
        dbname: str,
        table: str,
        schema: dict,
    ) -> Database:
        db = Database(dbname)
        db.create_table(name=table, schema=schema)
        return db
