from dataclasses import dataclass
from typing import Callable, Iterable
from abc import ABC, abstractmethod
#from auttcomp.extensions import Api as f
from ..sqlite import Entity, SqliteRepotoire, ExpressionApi as f

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
            self.my_entity = super().register(lambda: EntityA())

    repo = TestRepo(":memory:")

    repo.my_entity.add(EntityA(name="foo1", data=123))
    repo.my_entity.add(EntityA(name="foo2", data=456))
    repo.my_entity.add(EntityA(name="foo3", data=789))
    repo.my_entity.add(EntityA(*("test", 111)))
    
    #d2 = repo.my_entity.queryable() | f.map(lambda x: x.name) | list
    d2 = repo.my_entity.queryable() | f.map(lambda x: x.name)
    
    #print(d2())
