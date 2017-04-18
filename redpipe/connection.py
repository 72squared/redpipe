from redis.client import StrictPipeline
from .exceptions import AlreadyConnected, NotConfigured

__all__ = [
    'Connector',
    'connector',
    'connect_redis',
    'connect',
    'disconnect',
    'reset'
]

CONNECTION_DEFAULT_NAME = 'default'


def resolve_connection_name(name):
    return CONNECTION_DEFAULT_NAME if name is None else name


class Connector(object):
    __slots__ = ['connections']

    def __init__(self):
        self.connections = {}

    def get(self, name=None):
        name = resolve_connection_name(name)
        try:
            return self.connections[name]()
        except KeyError:
            raise NotConfigured('%s is not configured' % name)

    def connect(self, pipeline_method, name=None):

        name = resolve_connection_name(name)
        new_pool = pipeline_method().connection_pool
        try:
            if self.get(name).connection_pool != new_pool:
                raise AlreadyConnected("can't change connection for %s" % name)
        except NotConfigured:
            pass

        self.connections[name] = pipeline_method

    def connect_redis(self, redis_client, name=None,
                      transaction=True, shard_hint=None):
        connection_pool = redis_client.connection_pool
        response_callbacks = redis_client.response_callbacks

        def pipeline_method():
            return StrictPipeline(
                connection_pool=connection_pool,
                response_callbacks=response_callbacks,
                transaction=transaction,
                shard_hint=shard_hint
            )

        self.connect(pipeline_method=pipeline_method, name=name)

    def disconnect(self, name=None):
        name = resolve_connection_name(name)
        try:
            del self.connections[name]
        except KeyError:
            pass

    def reset(self):
        self.connections = {}


connector = Connector()
connect_redis = connector.connect_redis
connect = connector.connect
disconnect = connector.disconnect
reset = connector.reset
