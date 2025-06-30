import dis
from auttcomp.composable import Composable
from typing import Callable, Iterable

class Expression[T]:
    def __init__(self):
        pass
        #list of ops?
        
    def compile(self) -> str:
        '''
        converts expression tree to str
        '''
        pass

class ExpressionApi[T]:
    
    @staticmethod
    @Composable
    def filter[R](func: Callable[[T], R]) -> Callable[[Expression[T]], Expression[T]]:
        
        @Composable
        def partial_filter(data: Expression[T]) -> Expression[T]:
            #dis?
            return Expression()

        return partial_filter


class ExpressionBuilder[T]:
    pass
