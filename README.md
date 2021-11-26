# table
A "datatype" that sits between a dictionary and database


### Overview
`table` is a simple module for working with tabular data.
It's entirely driven by a single Python standard library `dataclasses.dataclass` object, but provides the user with a powerful SQL interface and fast, indexed, in-memory (or memory-mapped) performance out of the box.


### Motivation
When I was working accounting and finance jobs at the beginning of my career, MS Excel was the GOAT.
It gave me (and everyone around me) the power to persist and clean data, extract insights through visualizations, engage in "feature engineering", and so much more.
Excel also possessed some weaknesses as a result of the (intentional) tradeoffs required to make these strengths a reality;
most notably performance (which can suffer even with a few dozen MiB of data) and the "wild west" feel that comes with the freedom to mix arbitrary data types/structures, formulas, and formats (among other weirdness).

`table` was designed not to mimic Excel, but to take inspiration from some of its most user-friendly features.
At the same time, it incorporates some of the best advantages that a true programming language and genuine database have to offer.
The end result is something that attempts to capture the "niceness" of Excel and the performance of a database, while mitigating some of the most unpleasant characteristics of either one.
Think of it almost as the anti-`pandas` - a simple, easy to understand tool just for you and your data.


### What is it _not_?
`table` is not intended to act like a "real" database.
Each instance is its own mini db, and there is zero built-in support for things like multiple tables, joins, normalization, alternate backends (like PostgreSQL) etc.
It is most comfortable working with denormalized data.

Additionally, be aware that I've done little to stop you from shooting yourself in the foot. If you want to do something like `table.query("drop table _YOURTABLE_")`, nothing is going to stop you (for better or worse). I made the decision a while ago (at least for developer-facing tools) to never sacrifice simplicity or functionality in order to "save someone from themselves". We're all adults here, enjoy the freedom!


### Example
```python
from table import table
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
person = table(Person)
# person = table(Person, "person.db")  # creates the same table on disk

p1 = Person("Joe Schmo", 40, "100 Place Ln", "joe@schmo.com")
p2 = Person("Bill Bob", 60, "111 Cool Town", "bill@bob.com")
p3 = Person("Yackley Yoot", 25, "Bumblefartville")
records = [p1, p2, p3]

person.insert(records)

person.query("select * from person")
+--------------+-----+-----------------+---------------+----------------------------+
|     name     | age |     address     |     email     |         timestamp          |
+--------------+-----+-----------------+---------------+----------------------------+
|    Joe Schmo |  40 |    100 Place Ln | joe@schmo.com | 2021-11-20 12:42:19.246508 |
|     Bill Bob |  60 |   111 Cool Town |  bill@bob.com | 2021-11-20 12:42:19.246508 |
| Yackley Yoot |  25 | Bumblefartville |          None | 2021-11-20 12:42:19.246508 |
+--------------+-----+-----------------+---------------+----------------------------+

person.query("select avg(age) as avg_age from person")
+--------------------+
|      avg_age       |
+--------------------+
| 41.666666666666664 |
+--------------------+

# the output from the `query` method is an instance of `table.results.Results`.
# if you want the data underlying the tables above, you can access it via the `rows` attribute:
results = person.query("select * from person")
results.rows
[Row(name='Joe Schmo', age=40, address='100 Place Ln', email='joe@schmo.com', timestamp=datetime.datetime(2021, 11, 19, 21, 52, 28, 995979)),
 Row(name='Bill Bob', age=60, address='111 Cool Town', email='bill@bob.com', timestamp=datetime.datetime(2021, 11, 19, 21, 52, 28, 995979)),
 Row(name='Yackley Yoot', age=25, address='Bumblefartville', email=None, timestamp=datetime.datetime(2021, 11, 19, 21, 52, 28, 995979))]
```

For more examples, check out the [examples](./examples) directory.


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

##### VERSION 1
- [x] error handling and testing for erroneous inputs
- [x] check if table for given DB already exists
- [x] if db already exists, check that `dclass` matches `schema`
- [x] pretty print query results
- [x] create db method to dump in-mem table to disk
- [x] docstrings for API methods
- [x] examples directory
- [ ] ensure commits function properly

##### VERSION 2
- [x] create profiler base
- [ ] create benchmark base (with local DB to track history)
- [ ] write serializers to db in "private" table to be re-initialized on launch
- [ ] create history/audit table
- [ ] ? create "undo" functionality ?
