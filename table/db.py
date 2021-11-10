import logging
from collections import defaultdict, namedtuple
from dataclasses import dataclass
from datetime import date, datetime
from functools import partial, lru_cache
from hashlib import sha1
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
import sqlite3
from sqlite3 import Connection


LOGGER = logging.getLogger(__name__)
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
        execute(self._con, ddl)
        self._con.commit()

        return True

    def drop_table(self, name: str) -> bool:
        stmt = f"DROP TABLE {name}"
        execute(self._con, stmt)
        LOGGER.debug(stmt)

        serializers = self._serializers.get(name, [])
        for s in serializers:
            self._deregister_serializer(name, s)

    def create_index(
        self, 
        table: str, 
        columns: Union[str, List[str]]
    ) -> bool:
        stmt = "CREATE INDEX {idx_nm} ON {tbl_nm} ({cols})"

        idx_nm = index_name(table)
        if not isinstance(columns, list):
            columns = [columns]
        cols = ", ".join(columns)

        stmt = stmt.format(
            idx_nm=idx_nm,
            tbl_nm=table,
            cols=cols,
        )
        execute(self._con, stmt)
        return True

    def insert(
        self,
        table: str,
        schema: Dict[str, type],
        data: Union[tuple, List[tuple]],
    ) -> bool:
        stmt = insert_statement_from_schema(table, schema)
        execute(self._con, stmt, data)
        return True

    def execute(
        self, query: str, bind: Optional[tuple] = None
    ) -> List[Optional[tuple]]:
        return execute(self._con, query, bind)

    def schema(self, tablename: str) -> List[dict]:
        return get_schema(self._con, tablename)

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

        LOGGER.debug(f"Registered serializer [{serializer}]")

    def _deregister_serializer(
        self,
        table: str,
        serializer: Converter,
    ):
        self._serializers[table].remove(serializer)
        LOGGER.debug(
            f"Deregistered serializer [{serializer}]"
        )

    def _start(self):
        self._pre_config()
        self._con = create_db(self.db)
        self._post_config()

    def _pre_config(self):
        pass

    def _post_config(self):
        if not self._in_mem:
            config_mmap(self._con)


# ---------------------------------------------------------
def execute(
    con: Connection,
    query: str, 
    bind: Optional[Union[tuple, List[tuple]]] = None
) -> List[tuple]:
    if not bind:
        bind = ()
    
    executor = con.execute
    if isinstance(bind, list):
        executor = con.executemany
    
    cur = executor(query, bind)
    LOGGER.debug(query)

    output = cur.fetchall()
    desc = cur.description
    cur.close()
    con.commit()

    if not desc:
        return []

    cols = get_cols(desc)

    nt = nt_builder(cols)
    nt_row = lambda x: nt(*x)
    nt_output = list(map(nt_row, output))

    return nt_output


def get_schema(
    con: Connection, tablename: str
) -> List[dict]:
    q = f"PRAGMA table_info({tablename});"  # TODO: safe?
    cur = con.execute(q)
    LOGGER.debug(q)

    columns = [c[0] for c in cur.description]
    fields = cur.fetchall()
    cur.close()

    return schema_definition(
        columns=columns,
        fields=fields
    )


def create_db(db: str) -> Connection:
    con = sqlite3.connect(
        db, detect_types=sqlite3.PARSE_DECLTYPES
    )

    LOGGER.debug(f"Database created [{db}]")
    LOGGER.debug(f"Connection created [{con}]")
    
    return con


def config_mmap(
    con: Connection,
    page_size: int = 268435456
) -> None:
    stmt = f"PRAGMA mmap_size={page_size}"
    con.execute(stmt)
    LOGGER.debug(stmt)


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
    template = "CREATE TABLE IF NOT EXISTS {tname} ({schem})"

    col_def = partial(_col_def, mapping)
    schem = ", ".join(map(col_def, schema.items()))

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
    template = "INSERT INTO {tname} VALUES ({holdr})"

    holdr = _placeholder_def(schema)

    return template.format(
        tname=table_name,
        holdr=holdr
    )
    

def _placeholder_def(schema: Dict[type, str]) -> str:
    return ", ".join(["?"] * len(schema))


@lru_cache(maxsize=None)
def nt_builder(columns: Tuple[str]):
    # TODO: this chokes when doing things like summing
    # without an alias: select sum(age) from...
    nt = namedtuple("Row", columns)
    return nt


def index_name(table: str) -> str:
    short_hash = sha1(table.encode()).hexdigest()[:10]
    name = f"{table}_{short_hash}"
    return name


def get_cols(description):
    return tuple([
        d[0]
        for d in description
    ])
