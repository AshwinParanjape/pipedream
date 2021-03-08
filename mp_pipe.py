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

    def start(self):
        # The process's parent is only recorded when it is actually started
        super().__init__()
        signal.signal(signal.SIGPIPE, self.terminate_gracefully)
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
    def __init__(self, producer_processes: Iterator[PipeProcess] = None, *args, **kwargs):
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
        self.producer_processes = producer_processes

    def __iter__(self):
        # Create the producer process
        for p in self.producer_processes:
            p.start()
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
        for p in self.producer_processes:
            os.kill(p.pid, signal.SIGPIPE)

        # Wait till the current process has exited
        for p in self.producer_processes:
            p.join()
        return

class Pipeline:
    def __init__(self, instantiator, name: str):
        self.instantiator = instantiator
        self.name = name

    def instantiate(self) -> PipelineInstance:
        return self.instantiator()

    def chain(self, other: Pipeline):
        return Pipeline(instantiator=lambda: self.instantiate().chain(other.instantiate()),
                        name = ' | '.join([self.name, other.name]))

    def bypass(self, other: Pipeline):
        return Pipeline(instantiator=lambda: self.instantiate().bypass(other.instantiate()),
                        name = ' + '.join([self.name, other.name]))

    def feed(self, other: Iterable):
        return Pipeline(instantiator=lambda: self.instantiate()(other),
                        name = ' | '.join([str(type(other)), self.name]))

    def __call__(self, other: Iterable):
        return self.feed(other)

    def __ror__(self, other: Iterable):
        return self.feed(other)

    def __or__(self, other: Pipeline):
        return self.chain(other)

    def __add__(self, other: Pipeline):
        return self.bypass(other)

    def __iter__(self):
        return iter(self.instantiate())

class PipeFunction(Pipeline):
    def __init__(self, func: Callable[[Iterable], Iterable], name=None):
        self.func = func
        super().__init__(lambda: PipelineInstance.from_PipeFunction(self),
                         name or getattr(func, '__name__', str(type(func))))

class PipelineInstance:
    @classmethod
    def from_PipeFunction(cls, pipefunction: PipeFunction):
        tail_buffer = Buffer(maxsize=5)
        head_processes = [PipeProcess(function=pipefunction.func, output_buffer=tail_buffer)]
        tail_buffer.producer_processes = head_processes
        return cls(head_processes, tail_buffer)

    @property
    def head_buffer(self):
        return self._head_buffer

    @head_buffer.setter
    def head_buffer(self, head_buffer):
        self._head_buffer = head_buffer
        for hp in self.head_processes:
            hp.input_buffer = self._head_buffer

    def __init__(self, head_processes, tail_buffer):
        self.head_processes = head_processes
        self.tail_buffer = tail_buffer

    # def __call__(self, input_buffer: Iterable, output_buffer: Buffer = None, output_buffer_maxsize=5):
    def __call__(self, input_buffer: Iterable):
        self.head_buffer = input_buffer
        return self

    def chain(self, other: PipelineInstance):
        other.head_buffer = self.tail_buffer
        return PipelineInstance(self.head_processes, other.tail_buffer)

    def bypass(self, other: PipelineInstance):
        return PipelineInstance(self.head_processes + other.head_processes, self.tail_buffer)

    def __iter__(self):
        return iter(self.tail_buffer)



