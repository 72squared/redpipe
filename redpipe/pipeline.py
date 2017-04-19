import functools
from .result import DeferredResult
from .connection import connector, resolve_connection_name
from .exceptions import InvalidPipeline
from .async import promise, wait
__all__ = [
    'pipeline',
    'Pipeline',
    'NestedPipeline',
    'SuperPipeline'
]


class Pipeline(object):
    """
    Wrapper for redispy pipeline object.
    It returns a reference that contains a result
    once the pipeline executes.
    This allows us to be able to pipeline
    lots of calls within nested functions
    and not have to wait for the execute call.
    """
    __slots__ = ['connection_name', '_pipe', '_stack', '_callbacks', 'auto']

    def __init__(self, pipe, name=None, autocommit=False):
        """
        pass in the redispy client pipeline object.
        :param pipe: redis.StrictRedis.pipeline() or redis.Redis.pipeline()
        """
        self.connection_name = name
        self._pipe = pipe
        self._stack = []
        self._callbacks = []
        self.auto = autocommit

    def __getattr__(self, item):
        """
        magic method to intercept all calls bound for the internal
        client object and return a deferred result reference object.
        :param item: function name
        :return:
        """
        # get the original attribute from the client.
        # this could be a class method, or an instance attribute.
        if self._pipe is None:
            raise InvalidPipeline('not configured')

        f = getattr(self._pipe, item)

        # if it is just an attribute, return it.
        if not callable(f):
            return f

        # build a decorator for the internal method.
        # when the function is called, we create a DeferredResult object
        # and return it, and invoke the internal method against the
        # redispy pipeline object.
        # keep track of the ref object we return so we can put data into
        # the result on execute.
        @functools.wraps(f)
        def inner(*args, **kwargs):
            f(*args, **kwargs)
            ref = DeferredResult()
            self._stack.append(ref)
            return ref

        return inner

    def execute(self, raise_on_error=True):
        """
        Invoke the redispy pipeline.execute() method and take all the values
        returned in sequential order of commands and map them to the
        DeferredResult objects we returned when each command was queued inside
        the pipeline.
        Also invoke all the callback functions queued up.
        :param raise_on_error: boolean
        :return: None
        """
        stack = self._stack
        callbacks = self._callbacks
        self._stack = []
        self._callbacks = []
        if stack:
            res = self._pipe.execute(raise_on_error=raise_on_error)
            for i, v in enumerate(res):
                stack[i].set(v)
        for cb in callbacks:
            cb()

    def __enter__(self):
        """
        magic method to allow us to use in context like this:

            with Pipeline(redis.StrictRedis().pipeline()) as pipe:
                ref = pipe.set('foo', 'bar')
                pipe.execute()

        we are overriding the behavior in redispy.
        :return: Pipeline instance
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        context manager cleanup method.
        :param exc_type:
        :param exc_val:
        :param exc_tb:
        :return:
        """
        try:
            if exc_type is None and self.auto:
                self.execute()
        finally:
            if self._pipe is not None:
                self._pipe.__exit__(exc_type, exc_val, exc_tb)
            self.reset()

    def reset(self):
        """
        cleanup method. get rid of the stack and callbacks.
        :return:
        """
        if self._pipe is not None:
            self._pipe.reset()
        self._stack = []
        self._callbacks = []

    def on_execute(self, callback):
        """
        attach a callback to be called when the pipe finally executes.
        :param callback:
        :return:
        """
        self._callbacks.append(callback)


class NestedPipeline(object):
    __slots__ = ['connection_name', '_pipe', '_stack', '_callbacks', 'auto']

    def __init__(self, pipe, name=None, autocommit=False):
        self.connection_name = name
        self._pipe = pipe
        self._stack = []
        self._callbacks = []
        self.auto = autocommit

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
                ref.set(res.result)

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
        try:
            if exc_type is None and self.auto:
                self.execute()
        finally:
            self.reset()


class SuperPipeline(Pipeline):
    __slots__ = ['pipelines', '_pipe']

    def __init__(self, autocommit=False):
        try:
            pipe = connector.get()
        except InvalidPipeline:
            pipe = None
        super(SuperPipeline, self).__init__(pipe=pipe, autocommit=autocommit)
        self.pipelines = {n: Pipeline(c(), name=n)
                          for n, c in connector.connections.items()}

    def reset(self):
        super(SuperPipeline, self).reset()
        for c in self.pipelines.values():
            c.reset()

    def execute(self, raise_on_error=True):

        pipelines = self.pipelines
        promises = [promise(pipe.execute, raise_on_error=raise_on_error)
                    for pipe in pipelines.values()]
        wait(*promises)

        super(SuperPipeline, self).execute(raise_on_error=raise_on_error)


def pipeline(pipe=None, name=None, autocommit=False):
    if pipe is None and name is None:
        return SuperPipeline(autocommit=autocommit)

    name = resolve_connection_name(name)
    if pipe is None:
        return Pipeline(connector.get(name), name, autocommit=autocommit)

    try:
        # SuperPipeline object
        pipe = pipe.pipelines[name]
        return NestedPipeline(pipe, name, autocommit=autocommit)
    except (AttributeError, KeyError):
        pass

    try:
        if pipe.connection_name != name:
            raise InvalidPipeline(
                "%s and %s should match" % (pipe.connection_name, name))
    except AttributeError:
        raise InvalidPipeline('invalid pipeline object passed in')

    return NestedPipeline(pipe, name, autocommit=autocommit)
