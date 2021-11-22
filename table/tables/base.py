from table.db import (
    Database,
    DatabaseError,
)
from table.results import Results

import logging
from abc import ABC, abstractstaticmethod
from functools import partial
from typing import (
    List,
    Optional,
    TypeVar,
    Union,
)


__all__ = ["Table"]


LOGGER = logging.getLogger(__name__)
Dataclass = TypeVar("Dataclass")


class Table(ABC):
    def __init__(
        self,
        dclass: Dataclass,
        location: str,
    ) -> None:
        self.dclass = dclass
        self.location = location

        self._name = dclass.__name__.lower()
        self._schema = {
            col.lower(): typ
            for col, typ in dclass.__dict__[
                "__annotations__"
            ].items()
        }

        self._db: Database = self._connect(
            self.location, self._name, self._schema
        )

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
        output = self._db.execute(querystring, variables)
        results = Results(output)
        return results

    def index_column(self, column: str) -> bool:
        self._db.create_index(self._name, column)
        return True

    @abstractstaticmethod
    def _connect(
        dbname: str,
        table: str,
        schema: dict,
    ) -> Database:
        pass

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
