from dataclasses import dataclass
from repotoire.sqlite import Entity
from ..expression_builder import Expression, ExpressionApi

def test_expression_builder():

    @dataclass
    class TestEntity(Entity):
        name:str = None
        data_int:int = None
        
    api = ExpressionApi[TestEntity]()
    exp_builder = api.filter(lambda x: x.rowid == 1)
    exp_node = Expression("TestEntity")
    exp_builder(exp_node)
    query = exp_node.compile()
    assert query == "SELECT rowid, * FROM [TestEntity] WHERE rowid == 1"
