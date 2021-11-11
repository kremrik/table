from table import Table
from utils.cli import Arg, cli

from dataclasses import dataclass
from random import randint, seed
from string import ascii_lowercase
from sys import argv
from os import remove
from os.path import abspath, dirname, join


seed(13)


HERE = dirname(abspath(__file__))
DB_NAME = join(HERE, "profiler.db")


@dataclass
class Foo:
    letters: str
    number: int


def make_db():
    rows = 1_000
    table = Table(Foo, DB_NAME)

    rand_l = randint(1, 10)
    letters = "".join(_alpha_rand() for _ in range(rand_l))
    number = randint(1, 10000)

    records = []
    for _ in range(rows):
        rand_l = randint(1, 10)
        letters = "".join(_alpha_rand() for _ in range(rand_l))
        number = randint(1, 10000)
        record = Foo(letters, number)
        records.append(record)
    
    table.insert(records)


def destroy_db():
    remove(DB_NAME)


def _alpha_rand() -> str:
    rand = randint(0, 25)
    return ascii_lowercase[rand]


opts = [
    Arg(
        "setup", 
        make_db, 
        "Set up DB"
    ),
    Arg(
        "teardown", 
        destroy_db, 
        "Tear down db"
    )
]


if __name__ == "__main__":
    cli(argv[1:], opts)
