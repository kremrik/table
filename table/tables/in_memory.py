from table.db import (
    Database,
    DatabaseError,
    DatabaseWarning,
)
from table.errors import TableError
from table.tables.base import Table

import logging
from typing import TypeVar


__all__ = ["InMemoryTable"]


LOGGER = logging.getLogger(__name__)
Dataclass = TypeVar("Dataclass")


class InMemoryTable(Table):
    @staticmethod
    def _connect(
        dbname: str,
        table: str,
        schema: dict,
    ) -> Database:
        db = Database(dbname)
        try:
            db.create_table(name=table, schema=schema)
            return db
        except DatabaseWarning as e:
            print(e)
        except DatabaseError as e:
            raise TableError from e


# ---------------------------------------------------------
def format_insert(
    model: type,
    record: Dataclass,
) -> tuple:
    if not isinstance(record, model):
        msg = f"Data must be of type '{model}'"
        raise TypeError(msg)

    r = tuple(record.__dict__.values())
    return r
