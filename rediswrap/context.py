from .pipeline import Pipeline, NestedPipeline
from .connection import connector


class PipelineContext(object):
    """
    Allow us to declare a with block and automatically execute on exiting
    the block statement.

    example:
        with Pipe() as pipe:
            pipe.zadd(key, score, element)

    this is equivalent to writing:
        pipe = pipeline()
        pipe.zadd(key, score, element)
        pipe.execute()

    """
    __slots__ = ['_pipe']

    def __init__(self, pipe=None):
        self._pipe = Pipeline(connector.get()) if pipe is None \
            else NestedPipeline(pipe)

    def __enter__(self, pipe=None):
        return self._pipe

    def __exit__(self, type, value, traceback):
        if type is None:
            self._pipe.execute()
        self._pipe.reset()
