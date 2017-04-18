from .pipeline import pipeline

__all__ = [
    'PipelineContext',
]


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
        """
        set up a pipeline context.
        :param pipe: redpipe.Pipeline() or redpipe.NestedPipeline() or None
        :param name: str the name of the callable in the connector
        """
        self._pipe = pipeline(pipe, name)

    def __enter__(self):
        """
        enter the control-flow block.
        Do not call directly.
        :return: redpipe.Pipeline() or redpipe.NestedPipeline()
        """
        return self._pipe

    def __exit__(self, type, value, traceback):
        """
        Tear down the with control block.
        only execute the pipeline if there were no exceptions.
        Always reset it.
        Do not call directly.
        :param type:
        :param value:
        :param traceback:
        :return: None
        """
        if type is None:
            self._pipe.execute()
        self._pipe.reset()
