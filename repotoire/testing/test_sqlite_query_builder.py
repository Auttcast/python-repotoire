from dataclasses import dataclass
from ..sqlite import Entity, SqliteRepotoire, ExpressionApi as q

'''
from <source>
select <props> | <f(prop)> | distinct <prop>
where <condition>
...
join <source> on <match>
...
group <prop>
...
take

a queryable abstrction would be required for consistency with iterable join sources

.queryable() (or Queryable[T]) is like f.id, but taps the data source
the object is composable, but the compositions are all expressions until f.list or an f.<aggregate> is invoked


'''

def test_queryable():

    @dataclass
    class EntityA(Entity):
        name:str = None
        data:int = None
    
    class TestRepo(SqliteRepotoire):
        def __init__(self, connection_string):
            super().connect(connection_string)
            self.my_entity = super().register(EntityA)

    repo = TestRepo(":memory:")

    repo.my_entity.add(EntityA(name="foo1", data=123))
    repo.my_entity.add(EntityA(name="foo2", data=456))
    
    comp = repo.my_entity.queryable() | q.list

    actual = comp()
    e1 = actual[0]
    e2 = actual[1]

    assert e1.rowid == 1
    assert e1.name == "foo1"
    assert e1.data == 123

    assert e2.rowid == 2
    assert e2.name == "foo2"
    assert e2.data == 456

def test_queryable_map1():

    @dataclass
    class EntityA(Entity):
        name:str = None
        data:int = None
    
    class TestRepo(SqliteRepotoire):
        def __init__(self, connection_string):
            super().connect(connection_string)
            self.my_entity = super().register(EntityA)

    repo = TestRepo(":memory:")

    repo.my_entity.add(EntityA(name="foo1", data=123))
    repo.my_entity.add(EntityA(name="foo2", data=456))
    
    comp = repo.my_entity.queryable() | q.map(lambda x: x.name) | q.list
    
    actual = comp()

    assert actual == ["foo1", "foo2"]
    
def test_queryable_map2():

    @dataclass
    class EntityA(Entity):
        name:str = None
        data:int = None
    
    class TestRepo(SqliteRepotoire):
        def __init__(self, connection_string):
            super().connect(connection_string)
            self.my_entity = super().register(EntityA)

    repo = TestRepo(":memory:")

    repo.my_entity.add(EntityA(name="foo1", data=123))
    repo.my_entity.add(EntityA(name="foo2", data=456))
    
    comp = repo.my_entity.queryable() | q.map(lambda x: (x.name, x.data)) | q.list
    
    actual = comp()

    assert actual == [("foo1", 123), ("foo2", 456)]
    
def test_queryable_filter1():

    @dataclass
    class EntityA(Entity):
        name:str = None
        data:int = None
    
    class TestRepo(SqliteRepotoire):
        def __init__(self, connection_string):
            super().connect(connection_string)
            self.my_entity = super().register(EntityA)

    repo = TestRepo(":memory:")

    repo.my_entity.add(EntityA(name="foo1", data=123))
    repo.my_entity.add(EntityA(name="foo2", data=456))
    
    comp = repo.my_entity.queryable() | q.filter(lambda x: x.data < 200) | q.list
    
    actual = comp()

    assert len(actual) == 1

    e1 = actual[0]

    assert e1.rowid == 1
    assert e1.name == "foo1"
    assert e1.data == 123
    
def test_queryable_filter2_and():

    @dataclass
    class EntityA(Entity):
        name:str = None
        data:int = None
    
    class TestRepo(SqliteRepotoire):
        def __init__(self, connection_string):
            super().connect(connection_string)
            self.my_entity = super().register(EntityA)

    repo = TestRepo(":memory:")

    repo.my_entity.add(EntityA(name="foo1", data=123))
    repo.my_entity.add(EntityA(name="foo2", data=456))
    repo.my_entity.add(EntityA(name="foo3", data=789))
    
    comp = repo.my_entity.queryable() | q.filter(lambda x: x.data > 200 and x.data < 700) | q.list
    
    actual = comp()

    assert len(actual) == 1
    
    e1 = actual[0]

    assert e1.rowid == 2
    assert e1.name == "foo2"
    assert e1.data == 456
    
def test_queryable_filter2_or():

    @dataclass
    class EntityA(Entity):
        name:str = None
        data:int = None
    
    class TestRepo(SqliteRepotoire):
        def __init__(self, connection_string):
            super().connect(connection_string)
            self.my_entity = super().register(EntityA)

    repo = TestRepo(":memory:")

    repo.my_entity.add(EntityA(name="foo1", data=123))
    repo.my_entity.add(EntityA(name="foo2", data=456))
    repo.my_entity.add(EntityA(name="foo3", data=789))
    
    comp = repo.my_entity.queryable() | q.filter(lambda x: x.data == 123 or x.data == 789) | q.list
    
    actual = comp()

    assert len(actual) == 2
    
    e1 = actual[0]
    e2 = actual[1]

    assert e1.rowid == 1
    assert e1.name == "foo1"
    assert e1.data == 123

    assert e2.rowid == 3
    assert e2.name == "foo3"
    assert e2.data == 789

def test_queryable_iter():

    @dataclass
    class EntityA(Entity):
        name:str = None
        data:int = None
    
    class TestRepo(SqliteRepotoire):
        def __init__(self, connection_string):
            super().connect(connection_string)
            self.my_entity = super().register(EntityA)

    repo = TestRepo(":memory:")

    repo.my_entity.add(EntityA(name="foo1", data=123))
    repo.my_entity.add(EntityA(name="foo2", data=456))
    repo.my_entity.add(EntityA(name="foo3", data=789))
    
    comp = repo.my_entity.queryable() | q.filter(lambda x: x.data == 123 or x.data == 789)
    
    actual = iter(comp())

    e1 = next(actual)
    e2 = next(actual)

    assert e1.rowid == 1
    assert e1.name == "foo1"
    assert e1.data == 123

    assert e2.rowid == 3
    assert e2.name == "foo3"
    assert e2.data == 789
