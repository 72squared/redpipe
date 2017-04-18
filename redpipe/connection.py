from redis.client import StrictPipeline
from .exceptions import AlreadyConnected


class Connector(object):

    __slots__ = ['get']

    def connect(self, pipeline_method):
        try:
            if self.get().connection_pool != pipeline_method().connection_pool:
                raise AlreadyConnected("can't change connection.")
        except AttributeError:
            pass

        self.get = pipeline_method  # noqa

    def connect_redis(self, redis_client, transaction=True, shard_hint=None):
        connection_pool = redis_client.connection_pool
        response_callbacks = redis_client.response_callbacks

        def pipeline_method():
            return StrictPipeline(
                connection_pool=connection_pool,
                response_callbacks=response_callbacks,
                transaction=transaction,
                shard_hint=shard_hint
            )
        self.connect(pipeline_method=pipeline_method)

    def disconnect(self):
        try:
            del self.get
        except AttributeError:
            pass


connector = Connector()
connect_redis = connector.connect_redis
connect = connector.connect
disconnect = connector.disconnect
