# -*- coding: utf-8 -*-
"""
This is where the magic happens.
The most important components of redpipe are here.
The Pipeline and NestedPipeline classes and the pipeline function enable
Use to pass pipeline functions into each other and attach redis calls to them.

The main function exposed here is the `pipeline` function.
You will use it everywhere, so get used to this syntax:

.. code-block:: python

    def incr(name, pipe=None):
        with redpipe.autoexec(pipe=pipe) as pipe:
            return pipe.incr(name)

    with redpipe.autoexec() as pipe:
        a = incr('a', pipe=pipe)
        b = incr('b', pipe=pipe)

    print([a, b])

Look at the `incr` function.
The call to `redpipe.pipeline` will return a `Pipeline` object if None
is passed in. And if a Pipeline object is passed in, it will return a
`NestedPipeline` object. Those two objects present the same interface but
behave very differently.

`Pipeline` objects execute your pipelined calls.
`NestedPipeline` objects pass their commands up the chain to the parent
pipeline they wrap. This could be another `NestedPipeline` object, or
a Pipeline() object.
"""
from typing import (Union, Optional, Callable, Dict, List, Tuple)

# python 3.7 compatibility change
try:
    from typing import Protocol
except ImportError:
    from typing_extensions import Protocol  # type: ignore

from .futures import Future
from .connections import ConnectionManager  # noqa
from .tasks import TaskManager  # noqa
from .exceptions import InvalidPipeline

__all__ = [
    'pipeline',
    'autoexec',
    'PipelineInterface'
]


class PipelineInterface(Protocol):

    def execute(self) -> None: ...

    def on_execute(self, callback: Callable) -> None: ...

    def reset(self) -> None: ...

    def __getattr__(self, item: str): ...

    def __enter__(self): ...

    def __exit__(self, exc_type, exc_val, exc_tb) -> None: ...


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
    __slots__ = ['connection_name', 'autoexec', '_stack', '_callbacks',
                 '_pipelines', '_exit_handler']

    def __init__(self,
                 name: Optional[str],
                 autoexec: bool = False,  # noqa
                 exit_handler: Optional[Callable] = None):
        """
        Instantiate a new base pipeline object.
        This pipeline will be responsible for executing all the others that
        potentially get attached to it, including other named pipelines
        and any commands from nested pipelines.

        :param name: str    The name of the connection
        :param autoexec: bool, whether or not to implicitly execute the pipe.
        """
        self.connection_name: Optional[str] = name
        self._stack: List[Tuple[str, Tuple, Dict, Future]] = []
        self._callbacks: List[Callable] = []
        self.autoexec: bool = autoexec
        self._pipelines: Dict[str, Union[Pipeline, NestedPipeline]] = {}
        self._exit_handler: Optional[Callable] = exit_handler

    def __getattr__(self, item: str):
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
    def supports_redpipe_pipeline() -> bool:
        return True

    def _pipeline(self, name) -> PipelineInterface:
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
            pipe = Pipeline(name=name, autoexec=True)
            self._pipelines[name] = pipe
            return pipe

    def execute(self) -> None:
        """
        Invoke the redispy pipeline.execute() method and take all the values
        returned in sequential order of commands and map them to the
        Future objects we returned when each command was queued inside
        the pipeline.
        Also invoke all the callback functions queued up.
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
                pipe = ConnectionManager.get(self.connection_name)

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
            TaskManager.wait(*[TaskManager.promise(p) for p in promises])

        for cb in callbacks:
            cb()

    def __enter__(self) -> PipelineInterface:
        """
        magic method to allow us to use in context like this:

            with Pipeline(redis.Redis().pipeline()) as pipe:
                ref = pipe.set('foo', 'bar')
                pipe.execute()

        we are overriding the behavior in redispy.
        :return: Pipeline instance
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        context manager cleanup method.
        :param exc_type:
        :param exc_val:
        :param exc_tb:
        :return:
        """
        try:
            if exc_type is None and self.autoexec:
                self.execute()
        finally:
            self.reset()
            cb = self._exit_handler
            if cb:
                cb()

    def reset(self) -> None:
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

    def on_execute(self, callback: Callable) -> None:
        """
        attach a callback to be called when the pipe finally executes.
        :param callback:
        :return:
        """
        self._callbacks.append(callback)

    def _inject_callbacks(self, callbacks) -> None:
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
    __slots__ = ['connection_name', 'parent', 'autoexec', '_stack',
                 '_callbacks', '_exit_handler']

    def __init__(self,
                 parent: PipelineInterface,
                 name: Optional[str] = None,
                 autoexec: bool = False, # noqa
                 exit_handler: Optional[Callable] = None):
        """
        Similar interface to the Pipeline object, but with the ability
        to also track a parent pipeline object.
        :param parent: Pipeline() or NestedPipeline()
        :param name: str, the name of the connection
        :param autoexec: bool, implicitly call execute?
        """
        self.connection_name: Optional[str] = name
        self.parent: PipelineInterface = parent
        self._stack: List[Tuple[str, Tuple, Dict, Future]] = []
        self._callbacks: List[Callable] = []
        self.autoexec: bool = autoexec
        self._exit_handler: Optional[Callable] = exit_handler

    @staticmethod
    def supports_redpipe_pipeline() -> bool:
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

        build = self._nested_future

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

    @staticmethod
    def _nested_future(r, future):
        """
        A utility function to map one future result into
        another future via callback.
        :param r:
        :param future:
        :return:
        """

        def cb():
            future.set(r.result)

        return cb

    def reset(self):
        self._stack = []
        self._callbacks = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type is None and self.autoexec:
                self.execute()
        finally:
            self.reset()
            cb = self._exit_handler
            if cb:
                cb()


def pipeline(pipe: Optional[PipelineInterface] = None,
             name: Optional[str] = None,
             autoexec: bool = False, # noqa
             exit_handler: Optional[Callable] = None
             ) -> Union[Pipeline, NestedPipeline]:
    """
    This is the foundational function for all of redpipe.
    Everything goes through here.
    create pipelines, nest pipelines, get pipelines for a specific name.
    It all happens here.

    Here's a simple example:

    .. code:: python

        with pipeline() as pipe:
            pipe.set('foo', 'bar')
            foo = pipe.get('foo')
            pipe.execute()
        print(foo)
        > bar

    Now let's look at how we can nest a pipeline.

    .. code:: python

        def process(key, pipe=None):
            with pipeline(pipe, autoexec=True) as pipe:
                return pipe.incr(key)

        with pipeline() as pipe:
            key1 = process('key1', pipe)
            key2 = process('key2', pipe)
            pipe.execute()

        print([key1, key2])

        > [1, 1]


    :param pipe: a Pipeline() or NestedPipeline() object, or None
    :param name: str, optional. the name of the connection to use.
    :param autoexec: bool, if true, implicitly execute the pipe
    :param exit_handler: Callable
    :return: Pipeline or NestedPipeline
    """
    if pipe is None:
        return Pipeline(name=name, autoexec=autoexec,
                        exit_handler=exit_handler)

    try:
        if pipe.supports_redpipe_pipeline():
            return NestedPipeline(
                parent=pipe,
                name=name,
                autoexec=autoexec,
                exit_handler=exit_handler
            )
    except AttributeError:
        pass

    raise InvalidPipeline('check your configuration')


def autoexec(pipe: Optional[PipelineInterface] = None,
             name: Optional[str] = None,
             exit_handler: Optional[Callable] = None
             ) -> Union[Pipeline, NestedPipeline]:
    """
    create a pipeline with a context that will automatically execute the
    pipeline upon leaving the context if no exception was raised.

    :param pipe:
    :param name:
    :param exit_handler:
    :return:
    """
    return pipeline(pipe=pipe, name=name, autoexec=True,
                    exit_handler=exit_handler)
