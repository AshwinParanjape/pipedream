import pickle

from mp_pipe import PipeFunction
from functools import partial
import multiprocessing as mp
import timeit
import numpy as np
import math
import os
size = 2**12
input = np.random.rand(size, size)

split_matrix = PipeFunction(lambda it: enumerate(np.split(it[0], 4)), name='split_matrix')
element_op = lambda op: partial(map, lambda enumerated_matrix: (enumerated_matrix[0], op(enumerated_matrix[1])))
#sin_op = PipeFunction(element_op(lambda x: np.sin(np.cos(x))), name='sin_cos_op')
#sin_op = PipeFunction(element_op(lambda x: np.sin(x)), name='sin_op')
#op = np.frompyfunc(lambda x: math.log(math.sin(math.cos(math.sin(math.cos(math.log(x)))))), 1, 1)
op = np.log
sin_op = PipeFunction(element_op(op), name='sin_op')
join_matrix = PipeFunction(lambda it: [np.concatenate([e[1] for e in sorted(it, key=lambda e: e[0])])], name='join_matrix')
pickle.DEFAULT_PROTOCOL = 5
print(os.getpid())
pipeline = [input] | split_matrix | sin_op * 4 | join_matrix
def correctness():
    x = op(input)
    y = list(pipeline)[0]
    print(x.shape, y.shape)
    print((x==y).all())

def speed():
    print(timeit.timeit(lambda: op(input), number=1))
    print(timeit.timeit(lambda: list(pipeline)[0], number=1))

if __name__=='__main__':
    #correctness()
    speed()

