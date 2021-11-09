from table.db import Converter, Database

import sqlite3
from functools import partial
from typing import (
    List, 
    Optional, 
    TypeVar, 
    Union,
)


__all__ = ["Converter", "Table"]


Dataclass = TypeVar("Dataclass")


class Table:
    def __init__(
        self, 
        dclass: Dataclass,
        location: Optional[str] = None,
        serializers: Optional[List[Converter]] = None
    ) -> None:
        self.dclass = dclass
        self.location = location
        self.serializers = serializers

        self._name = dclass.__name__.lower()
        self._schema = dclass.__dict__["__annotations__"]
        self._db = None

        self._start()

    def insert(
        self, data: Union[Dataclass, List[Dataclass]]
    ) -> int:
        dclass_to_row = partial(format_insert, self.dclass)

        if isinstance(data, list):
            xformer = lambda x: map(dclass_to_row, x)
            count = len(data)
        else:
            xformer = dclass_to_row
            count = 1

        records = xformer(data)
        count = self._db.insert(
            table=self._name,
            schema=self._schema,
            data=records
        )

        return count

    def query(
        self,
        querystring: str, 
        variables: Optional[tuple] = None
    ) -> List[Optional[Dataclass]]:
        return self._db.execute(querystring, variables)
        
    def _start(self):
        db = Database(self.location)
        db.create_table(
            name=self._name,
            schema=self._schema,
            serializers=self.serializers,
        )
        self._db = db


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
