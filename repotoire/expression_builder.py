import dis
from auttcomp.composable import Composable
from typing import Callable, Iterable

class Expression[T]:
    def __init__(self):
        pass
        #list of ops?
        
    def compile(self):
        pass

class ExpressionApi[T]:
    
    @staticmethod
    @Composable
    def filter[T,R](func: Callable[[T], R]) -> Callable[[Iterable[T]], Iterable[T]]:
        
        @Composable
        def partial_filter(data: Iterable[T]) -> Iterable[T]:
            #todo dis
            pass

        return partial_filter


class ExpressionBuilder[T]:
    pass
