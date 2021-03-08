from mp_pipe import PipeFunction
from functools import partial
import itertools
def gt100(it):
    for i in it:
        if i > 100:
            yield i

def mul7(it):
    for i in it:
        if i % 7 == 0:
            yield i

def head10(it):
    n = 0
    for idx, i in enumerate(it):
        yield i
        if idx >= 9:
            return
gt100 = partial(filter, lambda i: i>100)
mul7 = partial(filter, lambda i: i%7==0)
head10 = lambda it: itertools.islice(it, 10)
gt100_p = PipeFunction(gt100, 'gt100')
mul7_p = PipeFunction(mul7, 'mul7')
head10_p = PipeFunction(head10, 'head10')
x = gt100_p | mul7_p + mul7_p | head10_p
print(x.name)
y = itertools.count(1) | x
print(y.name)
print(list(itertools.count(1) | x))
import psutil

current_process = psutil.Process()
children = current_process.children(recursive=True)
if len(children) == 0:
    print("No children left")
for child in children:
    print('Child pid is {}'.format(child.pid))
