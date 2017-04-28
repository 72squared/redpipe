from .futures import Future
from .connections import connector, resolve_connection_name
from .tasks import promise, wait
from .exceptions import InvalidPipeline

__all__ = [
    'pipeline',
]


def _nested_future(r, future):
    """
    A utility function to
    :param r:
    :param future:
    :return:
    """

    def cb():
        future.set(r.result)

    return cb


class Pipeline(object):
    """
    Wrapper for redispy pipeline object.
    It returns a reference that contains a result
    once the pipeline executes.
    This allows us to be able to pipeline
    lots of calls within nested functions
    and not have to wait for the execute call.

    Don't instantiate this class directly.
    Instead, use the redpipe.pipeline(pipe) function which
    will set up this object correctly.
    """
    __slots__ = ['connection_name', 'auto', '_stack', '_callbacks',
                 '_pipelines']

    def __init__(self, name, autocommit=False):
        """
        Instantiate a new base pipeline object.
        This pipeline will be responsible for executing all the others that
        potentially get attached to it, including other named pipelines
        and any commands from nested pipelines.

        :param name: str    The name of the connection
        :param autocommit: bool, whether or not to implicitly execute the pipe.
        """
        self.connection_name = name
        self._stack = []
        self._callbacks = []
        self.auto = autocommit
        self._pipelines = {}

    def __getattr__(self, item):
        """
        when you call a command like `pipeline().incr('foo')` it winds up here.
        the item would be 'incr', because python can't find that attribute.
        We build a custom function for it on the fly.

        :param item: str, the name of the function we are wrapping.
        :return: callable
        """

        def command(*args, **kwargs):
            """
            track all the arguments passed to this function along with the
            function name (item). That way when pipe.execute() happens, we'll
            be able to run it.
            Return a Future object that will eventually contain the result
            of a redis call.

            :param args: array
            :param kwargs: dict
            :return: Future
            """
            future = Future()
            self._stack.append((item, args, kwargs, future))
            return future

        return command

    @staticmethod
    def supports_redpipe_pipeline():
        return True

    def _pipeline(self, name):
        """
        Don't call this function directly.
        Used by the NestedPipeline class when it executes.
        :param name:
        :return:
        """
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
        Future objects we returned when each command was queued inside
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
                """
                take all the commands and pass them to redis.
                this closure has the context of the stack
                :return: None
                """

                # get the connection to redis
                pipe = connector.get(self.connection_name)

                # keep track of all the commands
                call_stack = []

                # build a corresponding list of the futures
                futures = []

                # we need to do this because we need to make sure
                # all of these are callable.
                # there shouldn't be any non-callables.
                for item, args, kwargs, future in stack:
                    f = getattr(pipe, item)
                    if callable(f):
                        futures.append(future)
                        call_stack.append((f, args, kwargs))

                # here's where we actually pass the commands to the
                # underlying redis-py pipeline() object.
                for f, args, kwargs in call_stack:
                    f(*args, **kwargs)

                # execute the redis-py pipeline.
                # map all of the results into the futures.
                for i, v in enumerate(pipe.execute()):
                    futures[i].set(v)

            promises.append(process)

        # collect all the other pipelines for other named connections attached.
        promises += [p.execute for p in self._pipelines.values()]
        if len(promises) == 1:
            promises[0]()
        else:
            # if there are no promises, this is basically a no-op.
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
    """
    Keep track of a parent pipeline object (either a `Pipeline` object or
    another `NestedPipeline` object.
    Queue the commands and pass them to the parent on execute.
    Don't instantiate this class directly.
    Instead, use the redpipe.pipeline(pipe) function which
    will set up this object correctly.
    """
    __slots__ = ['connection_name', 'parent', 'auto', '_stack', '_callbacks']

    def __init__(self, parent, name=None, autocommit=False):
        """
        Similar interface to the Pipeline object, but with the ability
        to also track a parent pipeline object.
        :param parent: Pipeline() or NestedPipeline()
        :param name: str, the name of the connection
        :param autocommit: bool, implicitly call execute?
        """
        self.connection_name = name
        self.parent = parent
        self._stack = []
        self._callbacks = []
        self.auto = autocommit

    @staticmethod
    def supports_redpipe_pipeline():
        """
        used by the `redpipe.pipeline()` function to determine if it can be
        nested inside other pipeline objects.
        Do not call directly.
        :return:
        """
        return True

    def __getattr__(self, item):
        """
        when you call a command like `pipeline(pipe).incr('foo')` it
        winds up here.
        the item would be 'incr', because python can't find that
        attribute.
        We build a custom function for it on the fly.

        :param item: str, the name of the function we are wrapping.
        :return: callable
        """

        def command(*args, **kwargs):
            """
            track all the arguments passed to this function along with the
            function name (item). That way when pipe.execute() happens, we'll
            be able to run it.
            Return a Future object that will eventually contain the result
            of a redis call.

            :param args: array
            :param kwargs: dict
            :return: Future
            """
            future = Future()
            self._stack.append((item, args, kwargs, future))
            return future

        return command

    def _pipeline(self, name):
        """
        Don't call directly.
        Used by other NestedPipeline objects.
        :param name:
        :return:
        """
        return getattr(self.parent, '_pipeline')(name)

    def execute(self):
        """
        execute the commands inside the nested pipeline.
        This causes all queued up commands to be passed upstream to the
        parent, including callbacks.
        The state of this pipeline object gets cleaned up.
        :return:
        """
        stack = self._stack
        callbacks = self._callbacks
        self._stack = []
        self._callbacks = []

        deferred = []

        build = _nested_future

        pipe = self._pipeline(self.connection_name)
        for item, args, kwargs, ref in stack:
            f = getattr(pipe, item)
            deferred.append(build(f(*args, **kwargs), ref))

        inject_callbacks = getattr(self.parent, '_inject_callbacks')
        inject_callbacks(deferred + callbacks)

    def on_execute(self, callback):
        """
        same purpose as the Pipeline().on_execute() method.
        In this case, it queues them so that when the nested pipeline
        executes,
        :param callback: callable
        :return: None
        """
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
    """
    This is the foundational function for all of redpipe.
    Everything goes through here.
    create pipelines, nest pipelines, get pipelines for a specific name.
    It all happens here.

    Here's a simple example:
        with pipeline() as pipe:
            pipe.set('foo', 'bar')
            foo = pipe.get('foo')
            pipe.execute()
        print(foo)
        > bar

    Now let's look at how we can nest a pipeline.
        def process(key, pipe=None):
            with pipeline(pipe, autocommit=True) as pipe:
                return pipe.incr(key)

        with pipeline() as pipe:
            key1 = process('key1', pipe)
            key2 = process('key2', pipe)
            pipe.execute()

        print([key1, key2])
        > [1, 1]


    :param pipe: a Pipeline() or NestedPipeline() object, or None
    :param name: str, optional. the name of the connection to use.
    :param autocommit: bool, if true, implicitly execute the pipe
    :return:
    """
    name = resolve_connection_name(name)
    if pipe is None:
        return Pipeline(name=name, autocommit=autocommit)

    try:
        if pipe.supports_redpipe_pipeline():
            return NestedPipeline(
                parent=pipe,
                name=name,
                autocommit=autocommit)
    except AttributeError:
        pass

    raise InvalidPipeline('check your configuration')