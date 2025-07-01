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
    
class ExpressionBuilder[T]:
    def __init__(self, shape:T):
        self.entity_name = shape.__class__.__name__
        self.expressions:list[Expression[T]] = [Expression(func=ExpressionType.SELECT, bytecode=lambda x: x, shape=shape)]

    def add(self, expression):
        self.expressions.append(expression)

    def build(self) -> str:
        query = f"SELECT rowid, * FROM [{self.entity_name}]"
        return query

class MapExpression[T,R](Composable):
    def __init__(self, func:Callable[[T], R]):
        super().__init__(func)
        self.func = func
        self.bytecode = [xi for xi in dis.Bytecode(func)]

    def __call__(self, builder:ExpressionBuilder[T]) -> ExpressionBuilder[R]:
        builder.add(Expression(func=ExpressionType.SELECT, bytecode=self.bytecode, shape=self.func(builder.expressions[-1])))
        return builder

class FilterExpression[T](Composable):
    def __init__(self, func: Callable[[T], bool]):
        super().__init__(func)
        self.func = func
        self.bytecode = [xi for xi in dis.Bytecode(func)]
        
    def __call__(self, builder:ExpressionBuilder[T]) -> ExpressionBuilder[T]:
        builder.add(Expression(func=ExpressionType.WHERE, bytecode=self.bytecode, shape=builder.expressions[-1].shape))
        return builder

class ExpressionApi:
    def __init__(self):
        self.map = MapExpression
        self.filter = FilterExpression
