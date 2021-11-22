from table import table

from datetime import datetime
from dataclasses import dataclass


"""
If you want to use variables in your queries, it's a good
idea to avoid the usual Python string substitution methods
(such as f-strings). This can open you up to a common
exploit known as "SQL injection". To safely get around this
problem, use the `variables` parameter on the `query`
method.
"""


@dataclass
class Person:
    name: str
    age: int
    address: str
    email: str = None
    timestamp: datetime = datetime.now()

person = table(Person)

p1 = Person("Joe Schmo", 40, "100 Place Ln", "joe@schmo.com")
p2 = Person("Bill Bob", 60, "111 Cool Town", "bill@bob.com")
p3 = Person("Yackley Yoot", 25, "Bumblefartville")
records = [p1, p2, p3]

person.insert(records)

person.query(
    "SELECT * FROM person LIMIT ? OFFSET ?",
    variables=(1, 1)
)
# +----------+-----+---------------+--------------+----------------------------+
# |   name   | age |    address    |    email     |         timestamp          |
# +----------+-----+---------------+--------------+----------------------------+
# | Bill Bob |  60 | 111 Cool Town | bill@bob.com | 2021-11-21 21:23:27.863117 |
# +----------+-----+---------------+--------------+----------------------------+
