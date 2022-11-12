import multiprocessing as mp
import time
from multiprocessing import shared_memory

def producer(q):
    ba = bytearray(B'hello')
    shm = shared_memory.SharedMemory(create=True, size=len(ba))
    shm.buf[:len(ba)] = ba
    q.put(shm.name)

def consumer(q):
    shm_name = q.get()
    shm = shared_memory.SharedMemory(create=False, name=shm_name)
    print(bytes(shm.buf[:5]))

if __name__ == '__main__':
    mp.set_start_method('fork')
    queue = mp.Queue(maxsize=1)
    producer_processs = mp.Process(target=producer, args=(queue,))
    consumer_process = mp.Process(target=consumer, args=(queue,))
    producer_processs.start()
    producer_processs.join()
    producer_processs.terminate()
    producer_processs.kill()
    time.sleep(1)
    consumer_process.start()
    consumer_process.join()

