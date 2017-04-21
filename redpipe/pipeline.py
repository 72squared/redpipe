from .result import Deferred
from .connection import connector, resolve_connection_name
from .tasks import promise, wait
from .exceptions import InvalidPipeline

__all__ = [
    'pipeline',
]


def _nested_deferred(r, ref):
    def cb():
        ref.set(r.result)

    return cb


class Pipeline(object):
    """
    Wrapper for redispy pipeline object.
    It returns a reference that contains a result
    once the pipeline executes.
    This allows us to be able to pipeline
    lots of calls within nested functions
    and not have to wait for the execute call.
    """
    __slots__ = ['connection_name', 'auto', '_stack', '_callbacks',
                 '_pipelines']

    def __init__(self, name, autocommit=False):
        """

        :param name:
        :param autocommit:
        """
        self.connection_name = name
        self._stack = []
        self._callbacks = []
        self.auto = autocommit
        self._pipelines = {}

    def __getattr__(self, item):
        def inner(*args, **kwargs):
            ref = Deferred()
            self._stack.append((item, args, kwargs, ref))
            return ref

        return inner

    @staticmethod
    def supports_redpipe_pipeline():
        return True

    def pipeline(self, name):
        if name == self.connection_name:
            return self

        try:
            return self._pipelines[name]
        except KeyError:
            pipe = Pipeline(name=name, autocommit=True)
            self._pipelines[name] = pipe
            return pipe

    def execute(self):
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

        promises = []
        if stack:
            def process():
                pipe = connector.get(self.connection_name)
                call_stack = []
                refs = []
                for item, args, kwargs, ref in stack:
                    f = getattr(pipe, item)
                    if callable(f):
                        refs.append(ref)
                        call_stack.append((f, args, kwargs))

                for f, args, kwargs in call_stack:
                    f(*args, **kwargs)

                for i, v in enumerate(pipe.execute()):
                    refs[i].set(v)

            promises.append(process)

        promises += [p.execute for p in self._pipelines.values()]
        if len(promises) == 1:
            promises[0]()
        else:
            wait(*[promise(p) for p in promises])

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
            self.reset()

    def reset(self):
        """
        cleanup method. get rid of the stack and callbacks.
        :return:
        """
        self._stack = []
        self._callbacks = []
        pipes = self._pipelines
        self._pipelines = {}
        for pipe in pipes.values():
            pipe.reset()

    def on_execute(self, callback):
        """
        attach a callback to be called when the pipe finally executes.
        :param callback:
        :return:
        """
        self._callbacks.append(callback)

    def _inject_callbacks(self, callbacks):
        self._callbacks[0:0] = callbacks


class NestedPipeline(object):
    __slots__ = ['connection_name', 'parent', 'auto', '_stack', '_callbacks']

    def __init__(self, parent, name=None, autocommit=False):
        self.connection_name = name
        self.parent = parent
        self._stack = []
        self._callbacks = []
        self.auto = autocommit

    @staticmethod
    def supports_redpipe_pipeline():
        return True

    def __getattr__(self, item):
        def inner(*args, **kwargs):
            ref = Deferred()
            self._stack.append((item, args, kwargs, ref))
            return ref

        return inner

    def pipeline(self, name):
        return self.parent.pipeline(name)

    def execute(self):
        stack = self._stack
        callbacks = self._callbacks
        self._stack = []
        self._callbacks = []

        deferred = []

        build = _nested_deferred

        pipe = self.parent.pipeline(self.connection_name)
        for item, args, kwargs, ref in stack:
            f = getattr(pipe, item)
            deferred.append(build(f(*args, **kwargs), ref))

        inject_callbacks = getattr(self.parent, '_inject_callbacks')
        inject_callbacks(deferred + callbacks)

    def on_execute(self, callback):
        self._callbacks.append(callback)

    def _inject_callbacks(self, callbacks):
        self._callbacks[0:0] = callbacks

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


def pipeline(pipe=None, name=None, autocommit=False):
    name = resolve_connection_name(name)
    if pipe is None:
        return Pipeline(name=name, autocommit=autocommit)
    else:
        try:
            if pipe.supports_redpipe_pipeline():
                return NestedPipeline(
                    parent=pipe,
                    name=name,
                    autocommit=autocommit)
        except AttributeError:
            pass

        raise InvalidPipeline('check your configuration')
