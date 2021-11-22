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
    """
    Create a table!

    If `location` is specified, the table will be created
    locally on disk (or loaded from disk, if it already
    exists). Otherwise, an in-memory table will be created.
    """
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
