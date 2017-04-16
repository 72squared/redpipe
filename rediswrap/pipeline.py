import functools
from .result import DeferredResult


class Pipeline(object):
    __slots__ = ['_client', '_stack', '_callbacks']

    def __init__(self, redis_client):
        self._client = redis_client
        self._stack = []
        self._callbacks = []

    def __getattr__(self, item):
        f = getattr(self._client, item)
        if not callable(f):
            return f

        @functools.wraps(f)
        def inner(*args, **kwargs):
            f(*args, **kwargs)
            ref = DeferredResult()
            self._stack.append(ref)
            return ref

        return inner

    def execute(self, raise_on_error=True):
        stack = self._stack
        callbacks = self._callbacks
        self._stack = []
        self._callbacks = []
        res = self._client.execute(raise_on_error=raise_on_error)
        for i, v in enumerate(res):
            stack[i].set(v)
        for cb in callbacks:
            cb()

    def on_execute(self, callable):
        self._callbacks.append(callable)


class NestedPipeline(object):
    __slots__ = ['_pipe']

    def __init__(self, pipe):
        self._pipe = pipe

    def __getattr__(self, item):
        return getattr(self._pipe, item)

    def execute(self, raise_on_error=True):
        pass
