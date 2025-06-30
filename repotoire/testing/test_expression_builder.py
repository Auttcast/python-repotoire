from dataclasses import dataclass
from repotoire.sqlite import Entity

def test_expression_builder():

    @dataclass
    class TestEntity(Entity):
        name:str = None
        data_int:int = None
        