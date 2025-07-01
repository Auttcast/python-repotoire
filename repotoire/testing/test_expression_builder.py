from dataclasses import dataclass
from repotoire.sqlite import Entity
from ..expression_builder import ExpressionApi, ExpressionApi, ExpressionBuilder

def test_expression_builder():

    @dataclass
    class TestEntity(Entity):
        name:str = None
        data_int:int = None
        
    api = ExpressionApi()
    exp_builder = ExpressionBuilder(TestEntity())
    api.filter[TestEntity](lambda x: x.rowid == 1)(exp_builder)
    query = exp_builder.build()
    assert query == "SELECT rowid, * FROM [TestEntity] WHERE rowid == 1"
