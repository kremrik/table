import marshal, types

def foo():
    return "hi"

m = marshal.dumps(foo.__code__)
with open("foo.bytes", "wb") as f:
    f.write(m)

with open("foo.bytes", "rb") as f:
    un_m = marshal.loads(f.read())
    foo = types.FunctionType(un_m, globals(), "foo")
