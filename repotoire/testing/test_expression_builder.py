import dis
from dataclasses import dataclass
from repotoire.sqlite import Entity
from ..expression_builder import Expression, ExpressionApi, ExpressionBuilder, ExpressionType

def test_expression_builder():

    @dataclass
    class TestEntity(Entity):
        name:str = None
        data_int:int = None
        
    api = ExpressionApi()
    exp_start = Expression(func=ExpressionType.SELECT, bytecode=dis.Bytecode(lambda x: x), shape=TestEntity())
    exp = api.filter(lambda x: x.rowid == 1)([exp_start])
    query = ExpressionBuilder(exp).build()
    assert query == "SELECT rowid, * FROM [TestEntity] WHERE rowid == 1"
