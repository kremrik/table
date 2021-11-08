import sqlite3
from collections import defaultdict, namedtuple
from dataclasses import dataclass
from datetime import date, datetime
from functools import partial, lru_cache
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


class DatabaseError(Exception):
    pass


@dataclass
class Converter:
    column: str
    type: type
    adapter: Callable[[Any], SQLiteType]
    converter: Callable[[bytes], Any]


class Database:
    def __init__(
        self,
        db: Optional[str] = None,
    ) -> None:
        self.db = db or ":memory:"

        self._serializers: Dict[str, Converter] = {}
        self._in_mem = not db
        self._con = None

        self._start()

    def create_table(
        self,
        name: str,
        schema: Dict[str, type],
        serializers: Optional[List[Converter]] = None,
    ) -> bool:
        # TODO: if a serializer is present, we need to use
        # that as the column type definition

        if not serializers:
            serializers = []

        for s in serializers:
            self._register_serializer(name, s)

        ddl = ddl_from_schema(
            table_name=name,
            schema=schema,
            mapping=TYPES,
        )
        self._con.execute(ddl)
        self._con.commit()

        return True

    def drop_table(self, name: str) -> bool:
        stmt = f"DROP TABLE {name}"
        self.execute(stmt)

        serializers = self._serializers.get(name, [])
        for s in serializers:
            self._deregister_serializer(name, s)

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

    def schema(self, tablename: str) -> List[dict]:
        q = f"PRAGMA table_info({tablename});"  # TODO: safe?
        cur = self._con.execute(q)
        columns = [c[0] for c in cur.description]
        fields = cur.fetchall()
        cur.close()

        return schema_definition(
            columns=columns,
            fields=fields
        )

    def _register_serializer(
        self,
        table: str,
        serializer: Converter,
    ):
        adapter = serializer.adapter
        converter = Converter.converter
        column = serializer.column
        obj_type = serializer.type

        sqlite3.register_adapter(obj_type, adapter)
        sqlite3.register_converter(column, converter)

        if table not in self._serializers:
            self._serializers[table] = []

        self._serializers[table].append(serializer)

    def _deregister_serializer(
        self,
        table: str,
        serializer: Converter,
    ):
        self._serializers[table].remove(serializer)

    def _start(self):
        self._pre_config()
        self._connect()
        self._post_config()

    def _pre_config(self):
        pass

    def _post_config(self):
        if not self._in_mem:
            self._con.execute("PRAGMA mmap_size=268435456")

    def _connect(self):
        con = sqlite3.connect(
            self.db, 
            detect_types=sqlite3.PARSE_DECLTYPES
        )
        self._con = con

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
