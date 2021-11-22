from table import table

import csv
from dataclasses import dataclass


"""
One common thing you may wish to do is move data from a CSV
file to a `table`. You will still need to work through your
dataclass "model", but this ensures that no weird rows show
up and cause strange errors to be thrown.

We'll assume you have a CSV that looks like the one below
(and note that there is no trailing newline which would
usually need to be accounted for):

$ cat foo.csv
foo,bar,baz
1,hi,bye
2,hello,goodbye
"""


@dataclass
class Foo:
    foo: int
    bar: str
    baz: str

foo = table(Foo)

with open("foo.csv", "r") as f:
    reader = csv.reader(f)
    next(reader)  # "skip" the header line
    for row in reader:
        foo.insert(Foo(*row))

foo.query("select * from foo")
# +-----+-------+---------+
# | foo |  bar  |   baz   |
# +-----+-------+---------+
# |   1 |    hi |     bye |
# |   2 | hello | goodbye |
# +-----+-------+---------+


# similarly, we can dump the contents of a table to a CSV:
with open("foo_again.csv", "w") as f:
    writer = csv.writer(f)
    results = foo.query("select * from foo")
    rows = results.rows  # using the underlying tuples
    for row in rows:
        writer.writerow(row)
