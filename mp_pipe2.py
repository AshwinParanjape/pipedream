from __future__ import annotations

import functools
import multiprocessing as mp
from multiprocessing.queues import Queue
import signal, os
from typing import Iterator, Callable, Iterable

mp.set_start_method('fork')

PoisonPill = "PoisonPill"

class PipeProcess(mp.Process):
    def __init__(self, function: Callable[[Iterable], Iterable], output_buffer: Queue, input_buffer: Iterable = None, ):
        """
        Extends multiprocessing.Process; function operates over input_buffer and prpushes objects into the output buffer

        Args:
            input_buffer: An iterable that is provided as input to the function
            function: Takes iterable as input and produces an iterable
            output_buffer: Holds the produced objects
        """
        self.function = function
        self.output_buffer = output_buffer
        self.input_buffer = input_buffer
        signal.signal(signal.SIGPIPE, self.terminate_gracefully)

    def start(self):
        # The process's parent is only recorded when it is actually started
        super().__init__()
        super().start()

    def terminate_gracefully(self, *args):
        for process in mp.active_children():
            os.kill(process.pid, signal.SIGPIPE)
            process.join()
        exit()

    def run(self):
        # self.function: iterator->iterator returns
        for item in self.function(self.input_buffer):
            self.output_buffer.put(item)

        # No more items, put PoisonPill so that downstream functions stop
        self.output_buffer.put(PoisonPill)
        self.output_buffer.close()


class Buffer(Queue):
    def __init__(self, producer_process: PipeProcess=None, *args, **kwargs):
        """
        An iterable multiprocessing.Queue containing objects produced by the producer process.
        Is reponsible for life cycle management of the producer process.
        Args:
            producer_process_init: A constructor for the producer process that writes to the buffer
            *args:
            **kwargs:
        """
        ctx = mp.get_context('fork')
        super().__init__(*args, **kwargs, ctx=ctx)
        self.producer_process = producer_process

    def __iter__(self):
        # Create the producer process
        self.producer_process.start()
        while True:

            # Get the next element from self (buffer). This element
            # was pushed into the buffer by the producer process
            i = self.get()
            if i != PoisonPill:
                yield i
            else:
                # If PoisonPill was sent by the producer, producer is one
                # exit iteration
                break

        # Send SIGPIPE to producer process. This triggers terminate_gracefully which
        # in turn kills all child processes spawned by self.process
        os.kill(self.producer_process.pid, signal.SIGPIPE)

        # Wait till the current process has exited
        self.producer_process.join()
        return

class PipeFunction:
    def __init__(self, func:Callable[[Iterable], Iterable]=None, instantiator=None):
        assert func or instantiator, "Either func or instantiator needs to be provided to PipeFunction"
        self.func = func
        self.__name__ = getattr(func, '__name__', type(func))
        self.instantiator = lambda: PipeFunctionInstance.from_PipeFunction(self)

    def __call__(self, other: Iterable):
        self.instantiator()(other)

    def __ror__(self, other: Iterable):
        return self.instantiator()(other)

    def __or__(self, other: PipeFunction):
        return PipeFunction(instantiator=lambda : PipeFunctionInstance.from_PipeFunction(self).compose(PipeFunctionInstance.from_PipeFunction(other)))

class PipeFunctionInstance:
    @classmethod
    def from_PipeFunction(cls, pipefunction: PipeFunction):
        tail_buffer = Buffer(maxsize=5)
        head_process = PipeProcess(function=pipefunction.func, output_buffer=tail_buffer)
        tail_buffer.producer_process = head_process
        return cls(head_process, tail_buffer)

    def __init__(self, head_process, tail_buffer):
        self.head_process = head_process
        self.tail_buffer = tail_buffer

    #def __call__(self, input_buffer: Iterable, output_buffer: Buffer = None, output_buffer_maxsize=5):
    def __call__(self, input_buffer: Iterable):
        self.head_process.input_buffer = input_buffer
        return self.tail_buffer

    def compose(self, other: PipeFunctionInstance):
        other.head_process.input_buffer=self.tail_buffer
        return PipeFunctionInstance(self.head_process, other.tail_buffer)



