import dis
from dataclasses import dataclass
from enum import Enum
from auttcomp.composable import Composable
from typing import Callable

class ExpressionType(Enum):
    SELECT=0,
    WHERE=1,
    # AGG_COUNT=2,
    # AGG_SUM=3,
    # AGG_AVG=4,

@dataclass
class Expression[T]:
    func:ExpressionType
    bytecode:list[dis.Instruction]
    shape:T
    
class ExpressionBuilder:
    def __init__(self, expression:Expression):
        self.expression = expression

    def build(self) -> str:
        query = f"SELECT rowid, * FROM [{self.expression}]"
        return query

class ExpressionApi:
    
    @staticmethod
    @Composable
    def map[T, R](func: Callable[[T], R]) -> Callable[[list[Expression[T]]], list[Expression[R]]]:
        
        bytecode = [xi for xi in dis.Bytecode(func)]
        
        @Composable
        def partial_filter(exp: list[Expression[T]]) -> list[Expression[R]]:
            exp.append(Expression(func=ExpressionType.SELECT, bytecode=bytecode, shape=func(exp[-1])))
            return exp

        return partial_filter
    
    @staticmethod
    @Composable
    def filter[T](func: Callable[[T], bool]) -> Callable[[list[Expression[T]]], list[Expression[T]]]:
        
        bytecode = [xi for xi in dis.Bytecode(func)]
        
        @Composable
        def partial_filter(exp: list[Expression]) -> list[Expression]:
            exp.append(Expression(func=ExpressionType.WHERE, bytecode=bytecode, shape=exp[-1].shape))
            return exp

        return partial_filter
