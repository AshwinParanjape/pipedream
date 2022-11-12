import os

print_debug = True
sys_print = print


def print(*args, **kwargs):
    if print_debug:
        sys_print(os.getpid(), *args, **kwargs)