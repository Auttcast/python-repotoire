import dis
import sqlite3
from abc import ABC
from typing import Callable, Iterable
from auttcomp.extensions import Api as f

class Entity:
    rowid:int

class SqliteRepotoire(ABC):

    def __init__(self):
        self.connection:sqlite3.Connection

    def connect(self, connection_string):
        self.connection = sqlite3.connect(connection_string)

    def __table_exists(self, cursor: sqlite3.Cursor, table_name:str):
        query = f"SELECT count(*) FROM sqlite_master WHERE [type]='table' and [name]='{table_name}'"
        (result,) = cursor.execute(query).fetchone()
        return result == 1

    def __assert_table_shape(self, cursor:sqlite3.Cursor, table_name:str, properties:list[str]):
        query = f"PRAGMA table_info([{table_name}])"
        info = list(map(lambda x: x[1], cursor.execute(query).fetchall()))
        assert info == properties

    def register[T](self, entity_factory:Callable[[], T]):
        entity = entity_factory()
        table_name = type(entity).__name__

        properties = list(filter(lambda x: x[0:2] != '__', dir(entity)))
        
        cursor = self.connection.cursor()

        if not self.__table_exists(cursor, table_name):
            query = f"CREATE TABLE {table_name} ({SqliteRepotoire.escaped_props_str(properties)})"
            cursor.execute(query)
        else:
            self.__assert_table_shape(cursor, table_name, properties)

        return SqliteRepoApi[T](table_name, properties, entity_factory, cursor)
    
    def escaped_props_str(props):
        return ",".join(list(map(lambda x: f"[{x}]", props)))

class SqliteRepoApi[T]:
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

    def __get_ordinal_prop_values(self, entity:T, properties:list[str]):
        str_props = list(map(lambda x: self.__str_adapter(getattr(entity, x)), properties))
        return ",".join(str_props)

    def add(self, entity:T):
        query = f"INSERT INTO [{self.table_name}] ({SqliteRepotoire.escaped_props_str(self.properties)}) VALUES ({self.__get_ordinal_prop_values(entity, self.properties)})"
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
        query = f"SELECT rowid, * FROM [{self.table_name}]"
        cur = self.cursor.execute(query)
        while result := cur.fetchone():
            yield self.__to_entity(result)
    