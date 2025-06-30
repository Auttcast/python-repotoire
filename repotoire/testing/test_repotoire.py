import sqlite3
from dataclasses import dataclass
from datetime import datetime
from ..sqlite import SqliteRepotoire, Entity

def test_sqlite_repotoire_create():

    #arrange

    @dataclass
    class TestEntity(Entity):
        name:str = None
        data_int:int = None
        data_float:float = None
        data_datetime:datetime = None
        data_bytes:bytearray = None

    class TestRepo(SqliteRepotoire):
        def __init__(self, connection_string):
            super().connect(connection_string)
            self.my_entity = super().register(lambda: TestEntity())

    repo = TestRepo(":memory:")

    #act
    
    repo.my_entity.add(TestEntity(name="test1", data_int=123, data_float=1.1, data_bytes=b"bytes1"))
    repo.my_entity.add(TestEntity(name="test2", data_int=456, data_float=1.02, data_bytes=b"bytes2"))
    
    #assert
    native_result = repo.connection.execute("SELECT rowid, name, data_int, data_float, data_bytes FROM TestEntity").fetchall()

    assert native_result[0][0] == 1 #rowid
    assert native_result[0][1] == 'test1' #name
    assert native_result[0][2] == 123 #data_int
    assert native_result[0][3] == 1.1 #data_float
    assert native_result[0][4] == b"bytes1" #data_bytes
    
    assert native_result[1][0] == 2
    assert native_result[1][1] == 'test2'
    assert native_result[1][2] == 456
    assert native_result[1][3] == 1.02
    assert native_result[1][4] == b"bytes2"
    
def test_sqlite_repotoire_read_all():
    
    #arrange

    @dataclass
    class TestEntity(Entity):
        name:str = None
        data_int:int = None

    class TestRepo(SqliteRepotoire):
        def __init__(self, connection_string):
            super().connect(connection_string)
            self.my_entity = super().register(lambda: TestEntity())

    repo = TestRepo(":memory:")

    #act
    
    repo.my_entity.add(TestEntity(name="test1", data_int=123))
    repo.my_entity.add(TestEntity(name="test2", data_int=456))
    
    #assert
    q = repo.my_entity.query()
    actual1 = next(q)
    actual2 = next(q)

    try:
        next(q)
        raise AssertionError("expected to throw")
    except StopIteration:
        pass

    assert actual1.rowid == 1 #rowid
    assert actual1.name == 'test1' #name
    assert actual1.data_int == 123 #data_int
    
    assert actual2.rowid == 2
    assert actual2.name == 'test2'
    assert actual2.data_int == 456

def test_assert_table_shape():
    
    @dataclass
    class TestEntity(Entity):
        name:str = None

    class TestRepo(SqliteRepotoire):
        def __init__(self):
            super().connect("file:mem1?mode=memory&cache=shared", uri=True)
            self.my_entity = super().register(lambda: TestEntity())

    TestRepo()

    @dataclass
    class TestEntity(Entity):
        name:str = None
        extra_property:int = None

    try:
        TestRepo()
        raise RuntimeError("expected exception")
    except AssertionError:
        pass

def xtest_sqlite_repotoire_read_filter():
    
    #arrange

    @dataclass
    class TestEntity(Entity):
        name:str = None
        data_int:int = None

    class TestRepo(SqliteRepotoire):
        def __init__(self, connection_string):
            super().connect(connection_string)
            self.my_entity = super().register(lambda: TestEntity())

    repo = TestRepo(":memory:")

    #act
    
    repo.my_entity.add(TestEntity(name="test1", data_int=123))
    repo.my_entity.add(TestEntity(name="test2", data_int=456))
    
    #assert
    q = repo.my_entity.query(lambda f: (
        f.filter(lambda x: x.rowid < 2)
    ))

    actual1 = next(q)

    try:
        next(q)
        raise AssertionError("expected to throw")
    except StopIteration:
        pass

    assert actual1.rowid == 1 #rowid
    assert actual1.name == 'test1' #name
    assert actual1.data_int == 123 #data_int
    

