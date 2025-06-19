from ..repotoire import SqliteRepotoire, Entity

class TestEntity(Entity):
    name:str = None
    data:int = None

class TestRepo(SqliteRepotoire):
    def __init__(self, connection_string):
        super().connect(connection_string)
        self.my_entity = super().register(lambda: TestEntity())

def test_sqlite_repotoire():

    repo = TestRepo(":memory:")
    test = TestEntity()

    test.name = "test"
    test.data = 123
    repo.my_entity.add(test)

    test.name = "test2"
    test.data = 456
    repo.my_entity.add(test)

    for x in repo.my_entity.get_all():
        print(f"VALUE: {x.name}")