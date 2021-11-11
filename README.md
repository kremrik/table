# table
A "datatype" that sits between a dictionary and database


### Overview
`table` is a simple module for working with tabular data.
It's entirely driven by a single Python standard library `dataclasses.dataclass` object, but provides the user with a powerful SQL interface and fast, indexed, in-memory (or memory-mapped on disk) performance out of the box.


### Examples
```python
from table import Table
from datetime import datetime
from dataclasses import dataclass

@dataclass
class Person:
    name: str
    age: int
    address: str
    email: str = None
    timestamp: datetime = datetime.now()

# create an in-memory table
person = Table(Person)
# person = Table(Person, "person.db")  # creates the same table on disk

p1 = Person("Joe Schmo", 40, "100 Place Ln", "joe@schmo.com")
p2 = Person("Bill Bob", 60, "111 Cool Town", "bill@bob.com")
p3 = Person("Yackley Yoot", 25, "Bumblefartville")
records = [p1, p2, p3]

person.insert(records)

person.query("select * from person")
[
    Row(name='Joe Schmo', age=40, address='100 Place Ln', email='joe@schmo.com', timestamp=datetime.datetime(2021, 11, 8, 20, 45, 4, 956300)),
    Row(name='Bill Bob', age=60, address='111 Cool Town', email='bill@bob.com', timestamp=datetime.datetime(2021, 11, 8, 20, 45, 4, 956300)),
    Row(name='Yackley Yoot', age=25, address='Bumblefartville', email=None, timestamp=datetime.datetime(2021, 11, 8, 20, 45, 4, 956300))
]

person.query("select avg(age) as avg_age from person")
[Row(avg_age=41.666666666666664)]
```


### A Friendly Warning
If you are working with a `Table`, be aware that I've done nothing to stop you from shooting yourself in the foot. If you want to do something like `table.query("drop table _YOURTABLE_")`, nothing is going to stop you (for better or worse). I made the decision a while ago (at least for developer-facing tools) to never sacrifice simplicity or functionality in order to "save someone from themselves". We're all adults here, enjoy the freedom!


### Installation (development)
```
git clone git@github.com:kremrik/table.git
cd table
pip install .
```


### Upgrading (development)
```
pip install . --upgrade
```


### TODO
- [ ] error handling and testing for erroneous inputs
- [ ] check if table for given DB already exists
- [ ] if db already exists, check that `dclass` matches `schema`
- [ ] write serializers to db in "private" table to be re-initialized on launch
- [ ] create history/audit table
- [ ] ? create process-local "undo" functionality ?
- [ ] create profiler base
- [ ] create benchmark base (with local DB to track history)
