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
        #self.input_queue.create_process()
        signal.signal(signal.SIGPIPE, self.terminate_gracefully)

    def terminate_gracefully(self, *args):
        for process in mp.active_children():
            os.kill(process.pid, signal.SIGPIPE)
            process.join()
        print(os.getpid(), f"{self.name} receieved SIGPIPE")
        if type(self.input_queue)==PipeFunction:
            print(os.getpid(), self.input_queue.name, "IS a PF")
            #self.input_queue.terminate()

        else:
            print(os.getpid(), self.input_queue, "Not PF")
        #self.output_buffer.close()
        exit()

    def run(self):
        proc_name = self.name
        print(f"{proc_name} started WrappedFunction for {self.function.__name__}")
        if type(self.input_queue) == PipeFunction:
            print(os.getpid(), f"{proc_name} starting iteration on {self.input_queue.name}")
        else:
            print(os.getpid(), f"{proc_name} starting iteration on {self.input_queue}")
        for item in self.function(self.input_queue):
            #print(item)
            self.output_buffer.put(item)
            #print(os.getpid(), "has input_queue", getattr(self.input_queue, 'name', self.input_queue))
        #except KeyboardInterrupt:
        #    print(f"{proc_name} receieved SIGINT")
        #    self.terminate_gracefully()
        self.output_buffer.put(PoisonPill)
        print("Added PosionPill")
        self.output_buffer.close()


class PipeFunction():
    def __init__(self, func):
        self.name = func.__name__
        self.func = func
        self.output_buffer = mp.Queue()

    def __iter__(self):
        print(os.getpid() ,"Creating Process to compute", self.name, "using iterator",  getattr(self.input_queue, 'name', self.input_queue))
        self.process = WrappedFunction(self.output_buffer, self.func, self.input_queue)
        print(os.getpid() , "of group", os.getpgid(os.getpid()), "Starting process")
        self.process.start()
        print("Process Started")

        while True:
            i = self.output_buffer.get()
            if i != PoisonPill:
                yield i
            else:
                print(os.getpid(), "Got poison pill")
                break
        print(os.getpid(), "Terminating process")
        self.terminate()
        print(os.getpid(), "Process Terminated")
        return

    def start(self):
        print(self.name, "starting its process")
        #if self.upstream_pfunction is not None:
        #    self.upstream_pfunction.start()
        self.process.start()
        print(self.name, "Done starting")


    def terminate(self):
        print(os.getpid(), self.name, "Terminating", self.process.pid)
        print(self.process.pid)
        #if type(self.input_queue)==PipeFunction:
            #self.input_queue.terminate()
        os.kill(self.process.pid, signal.SIGPIPE)
        self.process.join()
        #os.killpg(os.getpgid(self.process.pid), signal.SIGINT)
        #self.process.terminate()
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
