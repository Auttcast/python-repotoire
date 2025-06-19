import dis
import sqlite3
from datetime import datetime, UTC
from abc import ABC
from typing import Callable, Iterable
from auttcomp.extensions import Api as f
from .expression_builder import Expression, ExpressionBuilder

'''
TODO
-create with types
-indexing

TYPE MAP
None -> NULL
int -> INTEGER
float -> REAL
str -> TEXT
bytes -> BLOG


date:
store as FLOAT_DATETIME
adapter: datetime.now(UTC).timestamp()
converter: datetime.fromtimestamp
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

    def connect(self, connection_string):
        self.connection = sqlite3.connect(connection_string, detect_types=sqlite3.PARSE_DECLTYPES)
        sqlite3.register_adapter(datetime, SqliteRepotoire.adapt_datetime_to_float)
        sqlite3.register_converter("FLOAT_TIMESTAMP", SqliteRepotoire.convert_float_to_datetime)

    def __table_exists(self, cursor: sqlite3.Cursor, table_name:str):
        query = f"SELECT count(*) FROM sqlite_master WHERE [type]='table' and [name]='{table_name}'"
        (result,) = cursor.execute(query).fetchone()
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
            cursor.execute(f"CREATE TABLE [{table_name}] ({self.escaped_typed_props_str(entity, properties)})")
        else:
            self.__assert_table_shape(cursor, table_name, properties)

        return SqliteRepoApi[T](table_name, properties, entity_factory, cursor)
    
    @staticmethod
    def escaped_props_str(props):
        return ",".join([f"[{x}]" for x in props])

    def escaped_typed_props_str[T](self, entity:T, props:list[str]) -> str:
        print(f"TEST123: {props} {type(getattr(entity, props[1]))}")
        return ",".join([f"[{x}]" for x in props])

class SqliteRepoApi[T]:
    def __init__(self, table_name:str, properties:list[str], entity_factory:Callable[[], T], cursor:sqlite3.Cursor):
        self.table_name:str = table_name
        self.properties:list[str] = properties
        self.entity_factory:Callable[[], T] = entity_factory
        self.cursor:sqlite3.Cursor = cursor

    def add(self, entity:T):
        props = SqliteRepotoire.escaped_props_str(self.properties)
        prop_params = ",".join(["?" for _ in range(0, len(self.properties))])
        query = f"INSERT INTO [{self.table_name}] ({props}) VALUES ({prop_params})"
        self.cursor.execute(query, [getattr(entity, x) for x in self.properties])

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
    
    def query(expression:Expression = None) -> Iterable[T]:
        pass
