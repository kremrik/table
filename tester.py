from table import Table

import logging
from dataclasses import dataclass


fmt = "%(levelname)s | %(module)-5s | %(message)s"
logging.basicConfig(format=fmt)
logging.getLogger().setLevel(logging.DEBUG)


@dataclass
class Foo:
    name: str
    age: int

table = Table(Foo)

record1 = Foo("Joe", 30)
record2 = Foo("Bill", 40)
records = [record1, record2]

table.insert(records)
table.query("select sum(age) as sum_age from foo")
