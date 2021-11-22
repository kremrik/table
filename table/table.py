"""
There are two types of `Table`s that can be created:
  (1) in-memory, which is the simplest
  (2) persistent, which is durable and more complex
"""


from table.tables.base import Table
from table.tables.in_memory import InMemoryTable
from table.tables.persistent import PersistentTable

from typing import Optional, TypeVar


Dataclass = TypeVar("Dataclass")


def table(
    dclass: Dataclass,
    location: Optional[str] = None,
) -> Table:
    if not location:
        location = ":memory:"
        return InMemoryTable(
            dclass=dclass,
            location=location,
        )
    else:
        return PersistentTable(
            dclass=dclass,
            location=location,
        )
