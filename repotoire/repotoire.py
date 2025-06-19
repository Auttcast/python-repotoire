import dis
import sqlite3
from abc import ABC
from typing import Callable, Iterable
from auttcomp.extensions import Api as f
from auttcomp.composable import Composable
from pprint import pprint

class Entity:
    rowid:int

class Repotoire(ABC):

    def __init__(self):
        self.connection:sqlite3.Connection

    def connect(self, connection_string):
        self.connection = sqlite3.connect(connection_string)

    def __table_exists(self, cursor: sqlite3.Cursor, table_name:str):
        query = f"SELECT count(*) FROM sqlite_master WHERE [type]='table' and [name]='{table_name}'"
        (result,) = cursor.execute(query).fetchone()
        return result == 1

    def __assert_table_shape(self, cursor:sqlite3.Cursor, table_name:str, properties:list[str]):
        query = f"PRAGMA table_info({table_name})"
        info = list(map(lambda x: x[1], cursor.execute(query).fetchall()))
        assert info == properties

    def register[T](self, entity_factory:Callable[[], T]):
        entity = entity_factory()
        table_name = type(entity).__name__

        properties = list(filter(lambda x: x[0:2] != '__', dir(entity)))
        
        cursor = self.connection.cursor()

        if not self.__table_exists(cursor, table_name):
            query = f"CREATE TABLE {table_name} ({",".join(properties)})"
            cursor.execute(query)
        else:
            self.__assert_table_shape(cursor, table_name, properties)

        return RepoApi[T](table_name, properties, entity_factory, cursor)

class Queryable[T](Composable):
    def __init__(self):
        super().__init__(None)

class RepoApi[T]:
    def __init__(self, table_name:str, properties:list[str], entity_factory:Callable[[], T], cursor:sqlite3.Cursor):
        self.table_name:str = table_name
        self.properties:list[str] = properties
        self.entity_factory:Callable[[], T] = entity_factory
        self.cursor:sqlite3.Cursor = cursor

    def __str_adapter(self, prop):
        if type(prop) is str:
            return f"'{prop}'"
        elif type(prop) is type(None):
            return "NULL"
        else:
            return f"{prop}"

    def __get_ordinal_properties(self, entity:T, properties:list[str]):
        str_props = f.id(properties) > (
            f.map(lambda x: getattr(entity, x))
            | f.map(self.__str_adapter)
            | list
        )
        return ",".join(str_props)

    def add(self, entity:T):
        query = f"INSERT INTO {self.table_name} ({",".join(self.properties)}) VALUES ({self.__get_ordinal_properties(entity, self.properties)})"
        print(f"query: {query}")
        self.cursor.execute(query)

    def __to_entity(self, values) -> T:
        obj:T = self.entity_factory()
        setattr(obj, 'rowid', values[0])

        count = 1
        for p in self.properties:
            setattr(obj, p, values[count])
            count += 1
        return obj

    def get_all(self) -> Iterable[T]:
        query = f"SELECT rowid, * FROM {self.table_name}"
        objs = self.cursor.execute(query).fetchall()
        return f.id(objs) > f.map(self.__to_entity) | list
    
    def query(self) -> Queryable[T]:
        '''
        explore dis
        '''
        pass
