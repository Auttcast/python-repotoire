import dis
import sqlite3
from datetime import datetime, UTC
from abc import ABC
from typing import Callable, Iterable
from auttcomp.extensions import Api as f
from .expression_builder import Expression, ExpressionApi, ExpressionBuilder

'''
TODO
-entity will only support native types
--should assert types thru api (IE don't let something like datetime repr save as str, require user to be explicit)
-indexing
--bulk inserts

TYPE MAP
None -> NULL
int -> INTEGER
float -> REAL
str -> TEXT
bytes -> BLOB

'''

class Entity:
    rowid:int

class SqliteRepotoire(ABC):

    def __init__(self):
        self.connection:sqlite3.Connection

    @staticmethod
    def adapt_datetime_to_float(dt:datetime):
        return dt.astimezone(UTC).timestamp()

    @staticmethod
    def convert_float_to_datetime(timestamp):
        return datetime.fromtimestamp(timestamp)

    def connect(self, connection_string, uri=False):
        self.connection = sqlite3.connect(connection_string, uri=uri)

    def __table_exists(self, cursor: sqlite3.Cursor, table_name:str):
        query = f"SELECT count(*) FROM sqlite_master WHERE [type]='table' and [name]=?"
        #print(f"table_name:::: {table_name}")
        (result,) = cursor.execute(query, [table_name]).fetchone()
        return result == 1

    def __assert_table_shape(self, cursor:sqlite3.Cursor, table_name:str, properties:list[str]):
        query = f"PRAGMA table_info([{table_name}])"
        info = [x[1] for x in cursor.execute(query).fetchall()]
        assert info == properties

    def register[T](self, entity_factory:Callable[[], T]):
        entity = entity_factory()
        table_name = type(entity).__name__

        properties = list(filter(lambda x: x[0:2] != '__', dir(entity)))
        
        cursor = self.connection.cursor()

        if not self.__table_exists(cursor, table_name):
            cursor.execute(f"CREATE TABLE [{table_name}] ({self.escaped_props_str(properties)})")
        else:
            self.__assert_table_shape(cursor, table_name, properties)

        return SqliteRepoApi[T](table_name, properties, entity_factory, cursor)
    
    @staticmethod
    def get_places(props):
        return ",".join(["?" for _ in range(0, len(props))])

    @staticmethod
    def escaped_props_str(props):
        return ",".join([f"[{x}]" for x in props])

class SqliteRepoApi[T]:
    def __init__(self, table_name:str, properties:list[str], entity_factory:Callable[[], T], cursor:sqlite3.Cursor):
        self.table_name:str = table_name
        self.properties:list[str] = properties
        self.entity_factory:Callable[[], T] = entity_factory
        self.cursor:sqlite3.Cursor = cursor

    def add(self, entity:T):
        props = SqliteRepotoire.escaped_props_str(self.properties)
        places = ",".join(["?" for _ in range(0, len(self.properties))])
        query = f"INSERT INTO [{self.table_name}] ({props}) VALUES ({places})"
        self.cursor.execute(query, [getattr(entity, x) for x in self.properties])

    def __to_entity(self, values) -> T:
        obj:T = self.entity_factory()
        setattr(obj, 'rowid', values[0])

        count = 1
        for p in self.properties:
            setattr(obj, p, values[count])
            count += 1

        return obj

    def query(self, expression_builder:Callable[[ExpressionApi], Expression] = None) -> Iterable[T]:

        query = f"SELECT rowid, * FROM [{self.table_name}]" if expression_builder is None else expression_builder(ExpressionApi()).compile()

        cur = self.cursor.execute(query)
        while result := cur.fetchone():
            yield self.__to_entity(result)
