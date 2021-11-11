from table import Table
from profiler.dataclass_fixture import Foo


rows = 1_000
table = Table(Foo)

records = []
for _ in range(rows):
    record = Foo("abcdefg", 1234567)
    records.append(record)

table.insert(records)
