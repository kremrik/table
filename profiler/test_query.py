from table import Table
from profiler.dataclass_fixture import Foo

from os.path import abspath, dirname, join


HERE = dirname(abspath(__file__))
DB_NAME = join(HERE, "profiler.db")


table = Table(Foo, DB_NAME)


table.query("select avg(number) as mean from foo")
