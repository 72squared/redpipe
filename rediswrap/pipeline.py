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

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._client.__exit__(exc_type, exc_val, exc_tb)
        self._stack = []
        self._callbacks = []

    def reset(self):
        self._client.reset()
        self._stack = []
        self._callbacks = []

    def on_execute(self, callback):
        self._callbacks.append(callback)


class NestedPipeline(object):
    __slots__ = ['_pipe', '_stack', '_callbacks']

    def __init__(self, pipe):
        self._pipe = pipe
        self._stack = []
        self._callbacks = []

    def __getattr__(self, item):
        f = getattr(self._pipe, item)
        if not callable(f):
            return f

        @functools.wraps(f)
        def inner(*args, **kwargs):
            ref = DeferredResult()
            self._stack.append((f, args, kwargs, ref))
            return ref

        return inner

    def execute(self, raise_on_error=True):
        stack = self._stack
        callbacks = self._callbacks
        self._stack = []
        self._callbacks = []

        def build(res, ref):
            def cb():
                ref.result = res.result

            self._pipe.on_execute(cb)

        for f, args, kwargs, ref in stack:
            build(f(*args, **kwargs), ref)

        for cb in callbacks:
            self._pipe.on_execute(cb)

    def on_execute(self, callback):
        self._callbacks.append(callback)

    def reset(self):
        self._stack = []
        self._callbacks = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.reset()
