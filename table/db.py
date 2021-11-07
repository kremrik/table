import sqlite3
from collections import defaultdict, namedtuple
from dataclasses import dataclass
from datetime import date, datetime
from functools import cached_property, partial, lru_cache
from textwrap import dedent
from typing import (
    Any, 
    Callable, 
    Dict, 
    List, 
    Optional,
    Tuple,
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
        dbname: Optional[str] = None,  # TODO: should be allowed as the only param
        serializers: Optional[List[Converter]] = None,
    ) -> None:
        self.tablename = tablename
        self.fields = fields
        self.dbname = dbname or ":memory:"
        self.serializers = serializers

        self._in_mem = not dbname
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

    def insert_with_schema(
        self,
        data: Union[tuple, List[tuple]],
    ) -> None:
        stmt = insert_statement_from_schema(
            table_name=self.tablename,
            schema=self.fields,
        )

        if isinstance(data, list):
            self.executemany(stmt, data)
        else:
            self.execute(stmt, data)

    def execute(
        self, query: str, bind: Optional[tuple] = None
    ) -> list:
        if not bind:
            bind = ()
        
        cur = self._con.execute(query, bind)
        output = cur.fetchall()
        desc = cur.description
        cur.close()
        self._con.commit()

        if not desc:
            return []

        cols = self._get_cols(desc)

        nt = nt_builder(cols)
        nt_row = lambda x: nt(*x)
        nt_output = list(map(nt_row, output))

        return nt_output

    def executemany(self, query: str, bind:List[tuple]):
        # TODO: dry up with `execute`
        cur = self._con.executemany(query, bind)
        output = cur.fetchall()
        desc = cur.description
        cur.close()
        self._con.commit()

        if not desc:
            return []

        cols = self._get_cols(desc)

        nt = nt_builder(cols)
        nt_row = lambda x: nt(*x)
        nt_output = list(map(nt_row, output))

        return nt_output

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

        if not self._in_mem:
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

    @staticmethod
    def _get_cols(description):
        return tuple([
            d[0]
            for d in description
        ])


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


@lru_cache(maxsize=None)
def nt_builder(columns: Tuple[str]):
    nt = namedtuple("row", columns)
    return nt
