from table.db import Database
from table.tables.base import Table


__all__ = ["InMemoryTable"]


# TODO: duplicated from persistent.py
META_TABLE = "_metadata"
META_SCHEMA = {"tablename": str}


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

    def save(self, location: str) -> bool:
        """
        Write the in-memory table to disk
        """
        self._db.create_table(META_TABLE, META_SCHEMA)
        self._db.insert(
            META_TABLE, META_SCHEMA, (self._name,)
        )
        return self._db.backup(location)
