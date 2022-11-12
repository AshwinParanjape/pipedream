import multiprocessing as mp
import signal, os
mp.set_start_method('fork')

PoisonPill = "PoisonPill"

class WrappedFunction(mp.Process):
    def __init__(self, output_buffer, function, input_queue):
        super().__init__()
        self.function = function
        self.output_buffer = output_buffer
        self.input_queue = input_queue
        #signal.signal(signal.SIGINT, self.terminate_gracefully)


    def run(self):
        proc_name = self.name
        print(f"{proc_name} started WrappedFunction for {self.function.__name__}")
        for item in self.function(self.input_queue):
            #print(item)
            self.output_buffer.put(item)
        #except KeyboardInterrupt:
        #    print(f"{proc_name} receieved SIGINT")
        self.output_buffer.put(PoisonPill)
        print("Added PosionPill")
        self.output_buffer.close()


class PipeFunction():
    def __init__(self, func):
        self.name = func.__name__
        self.func = func
        self.output_buffer = mp.Queue()


    def __iter__(self):
        print(os.getpid() , "Starting process")
        print("Process Started")

        while True:
            i = self.output_buffer.get()
            if i != PoisonPill:
                yield i
            else:
                print("Terminating process")
                self.terminate()
                print("Process Terminated")
                return

    def start(self):
        print(self.name, "starting its process")
        #if self.upstream_pfunction is not None:
        #    self.upstream_pfunction.start()
        self.process.start()
        print(self.name, "Done starting")


    def terminate(self):
        print(self.name, "Terminating")
        #if type(self.input_queue)==PipeFunction:
            #self.input_queue.terminate()
        #os.kill(self.process.pid, signal.SIGINT)
        self.process.terminate()
        #if self.upstream_pfunction is not None:
            #self.upstream_pfunction.terminate()
        print(self.name, "Done Terminating")

    def __call__(self, input_queue, *args, **kwargs):
        self.output_buffer = mp.Queue(maxsize=5)
        self.input_queue = input_queue
        #if type(itr)==PipeFunction:
            #self.upstream_pfunction = itr
            #self.upstream_pfunction.start()
        #else:
            #self.upstream_pfunction = None
        self.process = WrappedFunction(self.output_buffer, self.func, self.input_queue)
        self.start()
        print(self.input_queue, hex(id(self.input_queue)))
        return self

    def __ror__(self, other):
        print(f"self: {self.name}")
        print("Ror")
        if type(other)==PipeFunction:
            print(f"other: {other.name}")
        return self(other)

    def __or__(self, other):
        print(f"self: {self.name}")
        print("or")
        if type(other)==PipeFunction:
            print(f"other: {other.name}")
        return other(self)
