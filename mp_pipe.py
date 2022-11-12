from __future__ import annotations

import multiprocessing as mp
from multiprocessing.queues import Queue, JoinableQueue
import signal, os
from typing import Iterator, Callable, Iterable
from ctypes import c_bool, c_int

from utils import print

mp.set_start_method('fork')


class PoisonPill:
    pass

#= "PoisonPill"


class PipeProcess(mp.Process):
    def __init__(self, function: Callable[[Iterable], Iterable], output_buffer: Queue, input_buffer: Iterable = None, name='', *args, **kwargs):
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
        self.args = args
        self.kwargs = kwargs
        self.name = name

    def start(self):
        # The process's parent is only recorded when it is actually started
        super().__init__(*self.args, **self.kwargs)
        signal.signal(signal.SIGPIPE, self.terminate_gracefully)
        #print("Starting", self.name)
        super().start()

    def terminate_gracefully(self, *args):
        for process in mp.active_children():
            os.kill(process.pid, signal.SIGPIPE)
            process.join()
        exit()

    def run(self):
        # self.function: iterator->iterator returns
        for item in self.function(self.input_buffer):
            #print(f"{self.name}, {os.getpid()} putting item {item}")
            self.output_buffer.put(item)
            #if isinstance(self.input_buffer, JoinableQueue):
            #    self.input_buffer.task_done()

        # No more items, put PoisonPill so that downstream functions stop
        #print(f"{self.name} adding PoisonPill")
        self.output_buffer.put(PoisonPill())
        print("Producer process finished, waiting to join")
        if isinstance(self.output_buffer, JoinableQueue):
            self.output_buffer.join()
        print("Producer process stopping")
        #


class BufferMixin():
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
        self.are_producers_created = mp.Value(c_bool, False)
        self.process_creator_instance = False
        self.producers_done = mp.Value(c_int, 0)
        self.waiting_buffer_iterators = mp.Value(c_int, 0)

    def __iter__(self):
        # Create the producer process
        #print(len(self.producer_processes))
        with self.are_producers_created.get_lock():
            if not self.are_producers_created.value:
                for p in self.producer_processes:
                    p.start()
                self.are_producers_created.value = True
                self.process_creator_instance = True
            else:
                pass
                #print("Producers already created")

        with self.waiting_buffer_iterators.get_lock():
            self.waiting_buffer_iterators.value += 1
        while True:
            # Get the next element from self (buffer). This element
            # was pushed into the buffer by the producer process
            i = self.get()
            if type(i) != PoisonPill:
                #print(f"yielding {i}")
                yield i
            else:
                # If PoisonPill was sent by the a producer, that producer is done
                with self.producers_done.get_lock():
                    self.producers_done.value += 1
                    #print("Producers done", self.producers_done.value, [p.name for p in self.producer_processes])

                    # If all producers are done, break
                    if self.producers_done.value >= len(self.producer_processes):
                        #print("breaking")
                        # add the PoisonPill back so that the queue iterator can exit from other processes
                        with self.waiting_buffer_iterators.get_lock():
                            print(os.getpid(), "waiting buffers", self.waiting_buffer_iterators.value)
                            self.waiting_buffer_iterators.value -= 1
                            if self.waiting_buffer_iterators.value>0:
                                self.put(i)
                        #self.task_done()
                        # exit iteration
                        break
                    else:
                        pass
                        #self.task_done()

        print(os.getpid(), "Iterator finished, waiting to join")
        self.join()

        # Send SIGPIPE to producer process. This triggers terminate_gracefully which
        # in turn kills all child processes spawned by self.process
        if self.process_creator_instance:
            for p in self.producer_processes:
                print("killing", p.pid)
                os.kill(p.pid, signal.SIGPIPE)

        # Wait till the current process has exited
        if self.process_creator_instance:
            for p in self.producer_processes:
                p.join()
        return

class Buffer(BufferMixin, JoinableQueue):
    pass

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

    def __mul__(self, n: int):
        x = self
        for i in range(n-1):
            x = x.bypass(self)
        return x


    def __iter__(self):
        print("Pipeline iter called")
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
        head_processes = [PipeProcess(function=pipefunction.func, output_buffer=tail_buffer, name=pipefunction.name)]
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
        #print(len(head_processes))
        self.head_processes = head_processes
        self.tail_buffer = tail_buffer

    # def __call__(self, input_buffer: Iterable, output_buffer: Buffer = None, output_buffer_maxsize=5):


    def chain(self, other: PipelineInstance):
        other.head_buffer = self.tail_buffer
        return PipelineInstance(self.head_processes, other.tail_buffer)

    def bypass(self, other: PipelineInstance):
        new_head_processes = self.head_processes + other.head_processes
        for p in new_head_processes:
            p.output_buffer = self.tail_buffer
        self.tail_buffer.producer_processes = new_head_processes
        other.tail_buffer = self.tail_buffer
        return PipelineInstance(new_head_processes, self.tail_buffer)

    def __call__(self, input_buffer: Iterable):
        self.head_buffer = input_buffer
        return self

    def __iter__(self):
        print("PipelineInstance iter called")
        for i in iter(self.tail_buffer):
            print("PipelineInstance marking task done")
            #self.tail_buffer.task_done()
            yield i




