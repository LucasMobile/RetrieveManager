"""Job functions run inside codec worker processes by tests."""

import os
import time


def echo(job):
    return (os.getpid(), job)


def crash(job):
    if job == "crash":
        os._exit(139)  # like a segfault in the native encoder
    return os.getpid()


def slow(seconds):
    time.sleep(seconds)
    return os.getpid()


def raises(_job):
    raise ValueError("bad job")
