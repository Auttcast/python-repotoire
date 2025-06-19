from dataclasses import dataclass
from datetime import datetime
import sqlite3
from ..repotoire import SqliteRepotoire, Entity

def test_sqlite_repotoire_create():

    #arrange

    @dataclass
    class TestEntity(Entity):
        name:str = None
        data_int:int = None
        data_float:float = None
        data_datetime:datetime = None

    class TestRepo(SqliteRepotoire):
        def __init__(self, connection_string):
            super().connect(connection_string)
            self.my_entity = super().register(lambda: TestEntity())

    repo = TestRepo(":memory:")

    #act

    repo.my_entity.add(TestEntity(name="test1", data_int=123))
    repo.my_entity.add(TestEntity(name="test2"))

    #assert
    native_result = repo.connection.execute("SELECT rowid, name, data_int, data_float, data_datetime FROM TestEntity").fetchall()

    assert native_result[0][0] == 1
    assert native_result[0][1] == 'test1'
    assert native_result[0][2] == 123
    
    assert native_result[1][0] == 2
    assert native_result[1][1] == 'test2'
    assert native_result[1][2] == None
    


def test_sqlite_repotoire_query():
    pass
