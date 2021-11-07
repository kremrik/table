import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from functools import cached_property, partial
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


class Database:
    def __init__(
        self,
        tablename: str,
        fields: dict,
        dbname: Optional[str] = None,
        in_memory: bool = True,
        serializers: Optional[List[Converter]] = None,
    ) -> None:
        self.tablename = tablename
        self.fields = fields
        self.dbname = ":memory:" if in_memory else dbname
        self.in_memory = in_memory
        self.serializers = serializers
        self._con = None

        self._start()

    @cached_property
    def schema(self) -> List[dict]:
        q = f"PRAGMA table_info({self.tablename});"
        cur = self._con.execute(q)
        columns = [c[0] for c in cur.description]
        fields = cur.fetchall()
        cur.close()

        return schema_definition(
            columns=columns,
            fields=fields
        )

    def execute(
        self, query: str, bind: Optional[tuple] = None
    ):
        if not bind:
            bind = ()
        
        cur = self._con.execute(query, bind)
        output = cur.fetchall()
        cur.close()

        return output

    def executemany(self, query: str, bind:List[tuple]):
        cur = self._con.executemany(query, bind)
        output = cur.fetchall()
        cur.close()
        return output

    def _start(self):
        self._connect()
        self._create()

    def _connect(self):
        # TODO: load adapters/converters
        con = sqlite3.connect(
            self.dbname, 
            detect_types=sqlite3.PARSE_DECLTYPES
        )
        self._con = con

        if not self.in_memory:
            self._con.execute("PRAGMA mmap_size=268435456")

    def _create(self):
        ddl = ddl_from_schema(
            table_name=self.tablename,
            schema=self.fields,
            mapping=TYPES,
        )
        print(ddl)
        self._con.execute(ddl)
        self._con.commit()


# ---------------------------------------------------------
def schema_definition(
    columns: List[str],
    fields: List[tuple],
) -> List[dict]:
    output = []
    for field in fields:
        d = dict(zip(columns, field))
        output.append(d)
    return output

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
