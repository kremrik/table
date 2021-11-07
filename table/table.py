import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from functools import partial
from textwrap import dedent
from typing import (
    Any, 
    Callable, 
    Dict, 
    List, 
    Optional, 
    TypeVar, 
    Union,
)


__all__ = ["Converter", "Table"]


Dataclass = TypeVar("Dataclass")
SQLiteType = Union[bytes, float, int, str]


TYPES = defaultdict(
    lambda: "BLOB",  # TODO: might be dumb
    {
        bytes: "TEXT",
        bool: "NUMERIC",
        date: "DATE",
        datetime: "TIMESTAMP",
        float: "REAL",
        int: "INTEGER",
        str: "TEXT",
    }
)


@dataclass
class Converter:
    column: str
    adapter: Callable[[Any], SQLiteType]
    converter: Callable[[bytes], Any]

    def __post_init__(self):
        # TODO: validate params
        pass


class Table:
    def __init__(
        self, 
        dclass: Dataclass,
        in_memory: bool = True,
        serializers: Optional[List[Converter]] = None
    ) -> None:
        self.dclass = dclass
        self.in_memory = in_memory
        self.serializers = serializers

        self._name = dclass.__name__.lower()
        self._schema = dclass.__dict__["__annotations__"]
        self._dbname = ":memory:" if in_memory else f"{self._name}.db"
        self._con = None

        self._start()

    def insert(
        self, data: Union[Dataclass, List[Dataclass]]
    ) -> int:
        if not isinstance(data, list):
            data = [data]

        # TODO: should be testable pure function
        insertable = []
        for d in data:
            if not isinstance(d, self.dclass):
                msg = f"Data must be of type '{self._name}'"
                raise TypeError(msg)
            record = tuple(d.__dict__.values())
            insertable.append(record)

        stmt = insert_statement_from_schema(
            table_name=self._name,
            schema=self._schema,
        )
        self._con.executemany(stmt, insertable)
        return len(data)

    def query(
        self, querystring: str
    ) -> List[Optional[Dataclass]]:
        results = []
        for row in self._con.execute(querystring):
            # TODO: should be testable
            item = self.dclass(*row)
            results.append(item)
        return results
        
    def _start(self):
        self._connect()
        self._create()

    def _connect(self):
        # TODO: load adapters/converters
        con = sqlite3.connect(
            self._dbname, 
            detect_types=sqlite3.PARSE_DECLTYPES
        )
        self._con = con

        if not self.in_memory:
            self._con.execute("PRAGMA mmap_size=268435456")

    def _create(self):
        ddl = ddl_from_schema(
            table_name=self._name,
            schema=self._schema,
            mapping=TYPES,
        )
        print(ddl)
        self._con.execute(ddl)
        self._con.commit()


# ---------------------------------------------------------
def ddl_from_schema(
    table_name: str,
    schema: Dict[str, type],
    mapping: Dict[type, str],
) -> str:
    template = dedent("""\
        CREATE TABLE IF NOT EXISTS {tname}
        (
            {schem}
        )\
    """)
    
    col_def = partial(_col_def, mapping)
    schem = ",\n    ".join(map(col_def, schema.items()))

    return template.format(
        tname=table_name,
        schem=schem
    )
    

def _col_def(
    mapping: Dict[type, str], schema_kv: tuple
) -> str:
    colname = schema_kv[0]
    _coltype = schema_kv[1]
    coltype = mapping[_coltype]
    return f"{colname} {coltype}"


def insert_statement_from_schema(
    table_name: str,
    schema: Dict[str, type],
) -> str:
    template = dedent("""\
        INSERT INTO {tname}
        VALUES ({holdr})
    """)

    holdr = _placeholder_def(schema)

    return template.format(
        tname=table_name,
        holdr=holdr
    )
    

def _placeholder_def(schema: Dict[type, str]) -> str:
    return ", ".join(["?"] * len(schema))
