from dataclasses import dataclass
import dis
from enum import Enum
from pprint import pprint
import sqlite3
from datetime import datetime, UTC
from abc import ABC, abstractmethod
from typing import Any, Callable, Iterable, Self
from auttcomp.extensions import Api as f
from auttcomp.composable import Composable
from collections import namedtuple

'''
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

@dataclass
class Expression[T]:
    func:Callable[[], T]=None
    bytecode:list[dis.Instruction]=None

class Queryable[T](ABC, Composable[None, T]):

    @abstractmethod
    def add(exp:Expression):
        pass

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

    def _to_entity(self, values) -> T:
        obj:T = self.entity_factory()
        setattr(obj, 'rowid', values[0])

        count = 1
        for p in self.properties:
            setattr(obj, p, values[count])
            count += 1

        return obj

    def queryable(self) -> Queryable[T]:
        return SqliteQueryable.from_table(self)

class DisPosition:

    def __init__(self, inst):
        self.registeres = []
        self.inst = list(map(lambda x: (x.opname, x.opcode, x.argval), inst))
        pprint(self.inst)
        first = self.inst.pop()
        assert first[1] == 36
        #resume 149
        #return_value 36

    def load_fast(self) -> Self:
        #85
        return self

    def load_attr(self) -> Self:
        #82
        return self

class QueryBuilder:
    def __init__(self, source:str, shape:list[str]):
        self.source = source
        self.shape = ['rowid', *shape]
        self.filters = []
        self.groupby = None

    def select(self, inst:list[dis.Instruction]):
        pprint(list(map(lambda x: (x.opname, x.opcode, x.argval), inst)))
        self.shape = list(
            map(
                lambda x: x.argval, filter(
                    lambda x: x[1] == 82, inst)
                    )
            )
        
    def where(self, inst:list[dis.Instruction]):
        #print(f"WHERE:::")
        #pprint(list(map(lambda x: (x.opname, x.opcode, x.argval), inst)))

        registers = []
        
        for i in inst:
            if i.opcode in [149, 85]: continue

            if i.opcode in [82, 83]:
                registers.append(i.argval)

            if i.opcode in [58]:
                right = registers.pop()
                left = registers.pop()
                registers.append(f"{left} {i.argval} {right}")

            if i.opcode == 97:
                registers.append(" AND ")
            if i.opcode == 100:
                registers.append(" OR ")

            if i.opcode == 36:
                self.filters.append("".join(registers))
                return

        raise SyntaxError("opcode 36 was not found")

    def group(self, inst:list[dis.Instruction]):
        pprint(list(map(lambda x: (x.opname, x.opcode, x.argval), inst)))
        i = list(
            map(
                lambda x: x.argval, filter(
                    lambda x: x[1] == 82, inst)
                    )
            )
        self.groupby = ", ".join(i)

    def order(self):
        pass

    def join(self):
        pass

    def build_where(self):

        if len(self.filters) == 0:
            return ""
        
        return f"WHERE {self.filters[0]}"

    def build_groupby(self):
        
        if self.groupby is None:
            return ""
        
        return f"GROUP BY {self.groupby}"


    def build(self) -> str:
        query = f"""
SELECT {', '.join(self.shape)} 
FROM [{self.source}]
{self.build_where()}
{self.build_groupby()}
        """
        print(f"QUERY: {query}")
        return query

class SqliteQueryable[T](Queryable[T], Iterable):

    def __init__(self, expressions:list[Expression], api:SqliteRepoApi[T]):
        Composable.__init__(self, lambda: self)
        self.expressions = expressions
        self.api = api
        self.iter = None
    
    def from_table(api:SqliteRepoApi[T]):
        return SqliteQueryable(api=api, expressions=[])
    
    def add(self, exp:Expression):
        return SqliteQueryable(api=self.api, expressions=[*self.expressions, exp])

    @staticmethod
    def tuple_row_factory(_, row):
        if type(row) is tuple and len(row) == 1:
            return row[0]
        return row

    def entity_row_factory(self):
        def partial_entity_row_factory(_, row):
            return self.api._to_entity(row)
        return partial_entity_row_factory

    def __iter__(self):
        self.iter = self()
        return self

    def __next__(self):
        return next(self.iter)

    def __call__(self) -> sqlite3.Cursor:
        
        builder = QueryBuilder(source=self.api.table_name, shape=self.api.properties)

        self.api.cursor.row_factory = self.entity_row_factory()

        for e in self.expressions:

            if e.func == ExpressionType.SELECT:
                builder.select(e.bytecode)
                self.api.cursor.row_factory = SqliteQueryable.tuple_row_factory

            elif e.func == ExpressionType.WHERE:
                builder.where(e.bytecode)

            elif e.func == ExpressionType.GROUP:
                builder.group(e.bytecode)
        
        cursor = self.api.cursor.execute(builder.build()) #cursor
        return cursor

class ExpressionType(Enum):
    SELECT=0,
    WHERE=1,
    GROUP=2

class ExpressionApi:

    @staticmethod
    @Composable
    def map[T, R](func:Callable[[T], R]) -> Callable[[Queryable[T]], Queryable[R]]:
        
        bytecode = list(dis.Bytecode(func))

        @Composable
        def partial_map(source:Queryable[T]) -> Queryable[R]:
            return source.add(Expression(func=ExpressionType.SELECT, bytecode=bytecode))

        return partial_map

    @staticmethod
    @Composable
    def filter[T](func:Callable[[T], bool]) -> Callable[[Queryable[T]], Queryable[T]]:
        
        bytecode = list(dis.Bytecode(func))

        @Composable
        def partial_filter(source:Queryable[T]) -> Queryable[T]:
            return source.add(Expression(func=ExpressionType.WHERE, bytecode=bytecode))

        return partial_filter

    @staticmethod
    @Composable
    def group[T, R](func:Callable[[T], R]) -> Callable[[Queryable[T]], Queryable[R]]:
        
        bytecode = list(dis.Bytecode(func))

        @Composable
        def partial_group(source:Queryable[T]) -> Queryable[R]:
            return source.add(Expression(func=ExpressionType.GROUP, bytecode=bytecode))

        return partial_group

    @staticmethod
    @Composable
    def list[T](source:Queryable[T]) -> list[T]:
        return list(source())
    