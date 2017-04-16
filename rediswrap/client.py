import functools
from .result import InstantResult
from .pipeline import Pipeline


class Client(object):

    def __init__(self, redis_client):
        self._client = redis_client

    def __getattr__(self, item):
        f = getattr(self._client, item)
        if not callable(f):
            return f

        @functools.wraps(f)
        def inner(*args, **kwargs):
            return InstantResult(f(*args, **kwargs))

        return inner

    def pipeline(self, transaction=True, shard_hint=None):
        return Pipeline(
            self._client.pipeline(transaction=transaction,
                                  shard_hint=shard_hint)
        )
