import copy
import pickle
from multiprocessing import shared_memory
from multiprocessing.queues import JoinableQueue
from typing import List

from mp_pipe import BufferMixin
from utils import print


class TrackedSharedMemoryObject():
    def __init__(self, obj):
        self.obj = obj

    def __getstate__(self):
        buffers: List[pickle.PickleBuffer]= []
        pobj = pickle.dumps(self.obj, protocol=5, buffer_callback=buffers.append)
        sm_buffers = []
        for buffer in buffers:
            sm_buffer = shared_memory.SharedMemory(create=True, size=buffer.raw().nbytes)
            sm_buffer.buf[:buffer.raw().nbytes] = buffer.raw()
            sm_buffers.append(sm_buffer)
        print( "Created", len(pobj), sm_buffers)
        sm_buffer_names = [smb.name for smb in sm_buffers]
        for b in sm_buffers:
            b.close()

        return {'pobj': pobj, 'sm_buffer_names': sm_buffer_names}

    def __setstate__(self, state):
        #print(state['pobj'])
        #print(state['sm_buffer_names'])
        sm_buffers = [shared_memory.SharedMemory(create=False, name=name) for name in state['sm_buffer_names']]
        print("Recreated", sm_buffers)
        #print([b.buf.nbytes for b in sm_buffers])
        self.obj = copy.deepcopy(pickle.loads(state['pobj'], buffers=[b.buf for b in sm_buffers]))
        for smb in sm_buffers:
            try:
                print('unlinking', smb)
                smb.unlink()
                smb.close()
            except:
                pass

        return self
        #for b in sm_buffers:
            #b.close()
            #b.unlink()


class SharedMemoryQueueMixin():
    #def __init__(self, *args, **kwargs):
    #    super().__init__(*args, **kwargs)
    #    self.shared_memory_manager = shared_memory.shared

    def put(self, item):
        item = SharedMemoryObject(item)
        super().put(item)

    def get(self):
        print( "Waiting In SMQM get")
        item = super().get()
        self.task_done()
        print("In SMQM got", item)
        return item.obj


class Buffer(BufferMixin, SharedMemoryQueueMixin, JoinableQueue):
    pass


def remove_shm_from_resource_tracker():
    """Monkey-patch multiprocessing.resource_tracker so SharedMemory won't be tracked

    More details at: https://bugs.python.org/issue38119
    """

    def fix_register(name, rtype):
        if rtype == "shared_memory":
            return
        return resource_tracker._resource_tracker.register(self, name, rtype)
    resource_tracker.register = fix_register

    def fix_unregister(name, rtype):
        if rtype == "shared_memory":
            return
        return resource_tracker._resource_tracker.unregister(self, name, rtype)
    resource_tracker.unregister = fix_unregister

    if "shared_memory" in resource_tracker._CLEANUP_FUNCS:
        del resource_tracker._CLEANUP_FUNCS["shared_memory"]


class SMBuffer(BufferMixin, SharedMemoryQueueMixin, JoinableQueue):
    pass