from table.db import Database
from table.errors import TableError
from table.tables.base import Table

from os.path import exists, getsize


__all__ = ["PersistentTable"]


MMAP_SIZE = 268435500  # 256MiB
META_TABLE = "_metadata"
META_SCHEMA = {"tablename": str}


class PersistentTable(Table):
    @staticmethod
    def _connect(
        dbname: str,
        table: str,
        schema: dict,
    ) -> Database:
        if not exists(dbname):
            mmap_size = MMAP_SIZE
            db = Database(dbname, mmap_size)
            db.create_table(META_TABLE, META_SCHEMA)
            db.insert(META_TABLE, META_SCHEMA, (table,))
            db.create_table(name=table, schema=schema)
            return db

        else:
            dbsize = getsize(dbname)
            mmap_size = max(
                MMAP_SIZE,
                round((dbsize / 1024) * 1.5) * 1024,
            )

            db = Database(dbname, mmap_size)

            if not db.table_exists(META_TABLE):
                db.create_table(META_TABLE, META_SCHEMA)
                db.insert(
                    META_TABLE, META_SCHEMA, (table,)
                )
                db.create_table(name=table, schema=schema)
            else:
                tablename = db.execute(
                    "select tablename from _metadata"
                )[0].tablename
                if tablename != table:
                    msg = f"Table '{table}' does not exist"
                    raise TableError(msg)

            return db
