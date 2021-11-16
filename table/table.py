from table.db import (
    Database,
    DatabaseError,
    DatabaseWarning,
)

import logging
from functools import partial
from typing import (
    List,
    Optional,
    TypeVar,
    Union,
)
from os.path import exists, getsize


__all__ = ["Table"]


LOGGER = logging.getLogger(__name__)
Dataclass = TypeVar("Dataclass")


class TableError(Exception):
    pass


class Table:
    def __init__(
        self,
        dclass: Dataclass,
        location: Optional[str] = None,
        index_columns: bool = False,
    ) -> None:
        self.dclass = dclass
        self.location = location
        self.index_columns = index_columns

        self._name = dclass.__name__.lower()
        self._schema = {
            col.lower(): typ
            for col, typ in dclass.__dict__[
                "__annotations__"
            ].items()
        }
        self._db: Database = None

        self._start()

    @property
    def schema(self) -> dict:
        db_schema = self._db.schema(self._name)
        columns = {
            col["name"]: col["type"] for col in db_schema
        }
        full = {"table": self._name, "columns": columns}
        return full

    def insert(
        self, data: Union[Dataclass, List[Dataclass]]
    ) -> int:
        dclass_to_row = partial(format_insert, self.dclass)

        if isinstance(data, list):
            xformer = lambda x: list(map(dclass_to_row, x))
            count = len(data)
        else:
            xformer = dclass_to_row
            count = 1

        records = xformer(data)
        count = self._db.insert(
            table=self._name,
            schema=self._schema,
            data=records,
        )

        return count

    def query(
        self,
        querystring: str,
        variables: Optional[tuple] = None,
    ) -> List[Optional[Dataclass]]:
        return self._db.execute(querystring, variables)

    def _start(self):
        self._create_db()
        self._create_table()
        if self.index_columns:
            self._create_index()

    def _create_db(self):
        mmap_size = None
        if self.location and exists(self.location):
            db_size = getsize(self.location)
            mmap_size = (
                round((db_size / 1024) * 1.5) * 1024
            )
            LOGGER.debug(f"db size: [{db_size}]")
            LOGGER.debug(f"mmap_size: [{mmap_size}]")

        db = Database(self.location, mmap_size)
        self._db = db

    def _create_table(self):
        try:
            self._db.create_table(
                name=self._name,
                schema=self._schema,
            )
        except DatabaseWarning as e:
            print(e)
        except DatabaseError as e:
            raise TableError from e

        LOGGER.debug(
            f"Table created with model [{self.dclass}]"
        )

    def _create_index(self):
        try:
            cols = list(self._schema)
            self._db.create_index(self._name, cols)
        except DatabaseError as e:
            LOGGER.error(e)
            print("Table already indexed, proceeding")


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
