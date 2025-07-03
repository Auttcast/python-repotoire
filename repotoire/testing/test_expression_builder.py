from dataclasses import dataclass
from repotoire.sqlite import Entity

def xtest_expression_builder():
    pass
    # @dataclass
    # class TestEntity(Entity):
    #     name:str = None
    #     data_int:int = None
        
    # api = ExpressionApi()
    # exp_builder = ExpressionBuilder(TestEntity())
    # api.filter[TestEntity](lambda x: x.rowid == 1)(exp_builder)
    # query = exp_builder.build()
    # assert query == "SELECT rowid, * FROM [TestEntity] WHERE rowid == 1"
