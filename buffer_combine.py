from mp_pipe import PipeFunction
from functools import partial
import itertools
import operator

mod7 = PipeFunction(partial(filter, lambda x: x%7==0), name='mod7')
mod13 = PipeFunction(partial(filter, lambda x: x%13==0), name='mod13')
head10 = PipeFunction(lambda it: itertools.islice(it, 12), name='head10')

print(list(itertools.count(1) | mod7 | head10))
print(list(itertools.count(1) | mod13 | head10))
o1 = itertools.count(1) | mod7
o2 = itertools.count(1) | mod13

def multit(*it):
    for els in zip(*it):
        yield list(itertools.accumulate(els, operator.mul))[-1]

multit_p = PipeFunction(multit, name='multit')
print(list([o1.instantiate(), o2.instantiate()] | multit_p | head10))



