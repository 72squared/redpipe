from .pipeline import Pipeline, NestedPipeline
from .connection import connector, resolve_connection_name
from .exceptions import InvalidPipeline


class PipelineContext(object):
    """
    Allow us to declare a with block and automatically execute on exiting
    the block statement.

    example:
        with PipelineContext() as pipe:
            pipe.zadd(key, score, element)

    this is equivalent to writing:
        pipe = context()
        pipe.zadd(key, score, element)
        pipe.execute()

    """
    __slots__ = ['_pipe']

    def __init__(self, pipe=None, name=None):
        self._pipe = pipeline(pipe, name)

    def __enter__(self):
        return self._pipe

    def __exit__(self, type, value, traceback):
        if type is None:
            self._pipe.execute()
        self._pipe.reset()


def pipeline(pipe=None, name=None):
    name = resolve_connection_name(name)
    if pipe is None:
        return Pipeline(connector.get(name), name)

    try:
        for p in pipe:
            if p.connection_name == name:
                pipe = p
                break
    except (AttributeError, TypeError):
        pass

    try:
        if pipe.connection_name != name:
            raise InvalidPipeline(
                "%s and %s should match" % (pipe.connection_name, name))
    except AttributeError:
        raise InvalidPipeline('invalid pipeline object passed in')

    return NestedPipeline(pipe, name)
