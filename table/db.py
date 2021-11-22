"""
This module could have been designed in an extensible way
in order to allow for connections to other databases.
However, the intention was to remain entirely within the
standard library. As Python only ships with one SQL
database (which is more than most languages can claim), the
need to support others was deemed unnecessary.
"""


import logging
from collections import namedtuple
from datetime import date, datetime
from functools import partial, lru_cache, wraps
from hashlib import sha1
from typing import (
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
)
import sqlite3
from sqlite3 import Connection, Error


LOGGER = logging.getLogger(__name__)
SQLiteType = Union[bytes, float, int, str]


TYPES = {
    bytes: "TEXT",
    bool: "NUMERIC",
    date: "DATE",
    datetime: "TIMESTAMP",
    float: "REAL",
    int: "INTEGER",
    str: "TEXT",
}


class DatabaseError(Exception):
    pass


class DatabaseWarning(Warning):
    pass


class Database:
    def __init__(
        self,
        db: Optional[str] = None,
        db_size: Optional[int] = None,
    ) -> None:
        self.db = db or ":memory:"
        self.db_size = db_size or 268435456

        self._in_mem = not db
        self._con = None
        self._tables: Dict[str, Dict[str, type]] = {}

        self._connect()

    def create_table(
        self,
        name: str,
        schema: Dict[str, type],
    ) -> bool:
        # TODO: this can probably be cleaned up

        types = list(TYPES)
        if diff := schema_is_invalid(schema, types):
            diff = list(diff)
            msg = f"Schema has disallowed types: {diff}; Allowed: {types}"
            raise DatabaseError(msg)

        if self.table_exists(name):
            _schem = self.schema(name)
            if not schemas_match(schema, _schem):
                msg = f"Table '{name}' exists, but schemas do not match"
                LOGGER.debug(f"Existing schema [{_schem}]")
                raise DatabaseError(msg)

            msg = f"Table '{name}' exists, proceeding"
            raise DatabaseWarning(msg)

        create_table(self._con, name, schema)

        self._tables[name] = schema

        return True

    def table_exists(self, name: str) -> bool:
        return table_exists(self._con, name)

    def drop_table(self, name: str) -> bool:
        return drop_table(self._con, name)

    def create_index(
        self, table: str, column: str
    ) -> bool:
        create_index(self._con, table, column)
        return True

    def insert(
        self,
        table: str,
        schema: dict,
        data: Union[tuple, List[tuple]],
    ) -> bool:
        stmt = insert_statement_from_schema(table, schema)
        execute(self._con, stmt, data)
        return True

    def execute(
        self, query: str, bind: Optional[tuple] = None
    ) -> List[Optional[tuple]]:
        return execute(self._con, query, bind)

    @lru_cache(maxsize=None)
    def schema(self, tablename: str) -> List[dict]:
        return get_schema(self._con, tablename)

    def backup(self, location: str) -> bool:
        backup(self._con, location)
        return True

    def _connect(self):
        self._pre_config()
        self._con = create_db(self.db)
        self._post_config()

    def _pre_config(self):
        pass

    def _post_config(self):
        if not self._in_mem:
            config_mmap(self._con, self.db_size)


# ---------------------------------------------------------
def fwdexception(fnc: Callable):
    @wraps(fnc)
    def wrapper(*args, **kwargs):
        try:
            return fnc(*args, **kwargs)
        except Error as e:
            raise DatabaseError from e

    return wrapper


@fwdexception
def execute(
    con: Connection,
    query: str,
    bind: Optional[Union[tuple, List[tuple]]] = None,
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


@fwdexception
def table_exists(con: Connection, name: str) -> bool:
    stmt = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{name}'"
    cur = con.execute(stmt)
    res = cur.fetchone()

    LOGGER.debug(stmt)

    if res:
        return True
    return False


@fwdexception
def create_table(
    con: Connection,
    name: str,
    schema: Dict[str, type],
    mapping: Optional[dict] = None,
) -> None:
    if not mapping:
        mapping = TYPES

    ddl = ddl_from_schema(
        table_name=name,
        schema=schema,
        mapping=mapping,
    )

    con.execute(ddl)
    con.commit()
    LOGGER.debug(ddl)


@fwdexception
def drop_table(con: Connection, name: str) -> None:
    stmt = f"DROP TABLE {name}"
    con.execute(stmt)
    LOGGER.debug(stmt)


@fwdexception
def create_index(
    con: Connection,
    table: str,
    column: str,
) -> None:
    stmt = "CREATE INDEX {idx_nm} ON {tbl_nm} ({col})"

    idx_nm = f"idx_{table}_{column}"

    stmt = stmt.format(
        idx_nm=idx_nm,
        tbl_nm=table,
        col=column,
    )
    con.execute(stmt)
    LOGGER.debug(stmt)


@fwdexception
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
        columns=columns, fields=fields
    )


@fwdexception
def create_db(db: str) -> Connection:
    con = sqlite3.connect(
        db, detect_types=sqlite3.PARSE_DECLTYPES
    )

    LOGGER.debug(f"Database created [{db}]")
    LOGGER.debug(f"Connection created [{con}]")

    return con


@fwdexception
def config_mmap(con: Connection, page_size: int) -> None:
    # https://www.sqlite.org/mmap.html
    stmt = f"PRAGMA mmap_size={page_size}"
    con.execute(stmt)
    LOGGER.debug(stmt)


@fwdexception
def backup(con: Connection, location: str) -> None:
    bak_con = sqlite3.connect(location)
    with bak_con:
        con.backup(bak_con)
    LOGGER.debug(f"Database dumpted [{location}]")


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
    template = (
        "CREATE TABLE IF NOT EXISTS {tname} ({schem})"
    )

    col_def = partial(_col_def, mapping)
    schem = ", ".join(map(col_def, schema.items()))

    return template.format(tname=table_name, schem=schem)


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

    return template.format(tname=table_name, holdr=holdr)


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
    return tuple([d[0] for d in description])


def schemas_match(given: dict, have: List[dict]) -> bool:
    # TODO: flesh out with types eventually

    given_cols = set(given)

    have_cols = set([col["name"] for col in have])

    if given_cols - have_cols or have_cols - given_cols:
        return False

    return True


def schema_is_invalid(
    given: dict, allowed: List[type]
) -> Optional[set]:
    s_given = set(given.values())
    s_allowed = set(allowed)

    diff = s_given - s_allowed
    if diff:
        return diff
