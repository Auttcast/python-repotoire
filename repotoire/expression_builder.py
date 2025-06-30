import dis
from auttcomp.composable import Composable
from typing import Callable

class Expression:
    def __init__(self, table_name):
        self.table_name = table_name
        self.code = []
        
    def add(self, next_func_bytecode):
        self.code.append(next_func_bytecode)

    def compile(self) -> str:
        query = f"SELECT rowid, * FROM [{self.table_name}]"
        return query

class ExpressionApi[T]:
    
    @staticmethod
    @Composable
    def filter(func: Callable[[T], bool]) -> Callable[[Expression], Expression]:
        
        func_bytecode = [xi for xi in dis.Bytecode(func)]
        
        @Composable
        def partial_filter(exp: Expression) -> Expression:
            exp.add(func_bytecode)
            return exp

        return partial_filter

