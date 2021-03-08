class PipeFunction():
    def __init__(self, func):
        self.func = func

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)

    def __ror__(self, other):
        return self(other)
