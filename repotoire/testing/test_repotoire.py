from ..repotoire import Repotoire, Entity

class TestEntity(Entity):
    name = None
    data = None

class MyRepo(Repotoire):
    def __init__(self, connection_string):
        super().connect(connection_string)
        self.my_entity = super().register(lambda: TestEntity())

def test_sqlite():

    '''
    TODO
    -is queryable possible with dis?
    -must validate with test suite and only support minimal operations
    -anticipage verion-specific mods
    '''

    repo = MyRepo(":memory:")

    test = TestEntity()

    test.name = "test"
    test.data = 123
    repo.my_entity.add(test)

    test.name = "test2"
    test.data = 456
    repo.my_entity.add(test)
