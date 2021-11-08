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
        self._db

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

        self._db.insert_with_schema(xformer(data))
        return count

    def query(
        self,
        querystring: str, 
        variables: Optional[tuple] = None
    ) -> List[Optional[Dataclass]]:
        results = []
        for row in self._con.execute(querystring):
            # TODO: should be testable
            item = self.dclass(*row)
            results.append(item)
        return results
        
    def _start(self):
        db = Database(
            tablename=self._name,
            fields=self._schema,
            dbname=self.location,
            serializers=self.serializers,
        )
        self._db = db


# ---------------------------------------------------------
def format_insert(
    model: type,
    record: Dataclass,
) -> tuple:
    if not isinstance(d, model):
        msg = f"Data must be of type '{model}'"
        raise TypeError(msg)
    
    r = tuple(record.__dict__.values())
    return r
