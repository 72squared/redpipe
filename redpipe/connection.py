from redis.client import StrictPipeline
from .exceptions import AlreadyConnected, InvalidPipeline

__all__ = [
    'ConnectionManager',
    'connector',
    'connect_redis',
    'connect',
    'disconnect',
    'reset'
]

CONNECTION_DEFAULT_NAME = 'default'


def resolve_connection_name(name=None):
    return CONNECTION_DEFAULT_NAME if name is None else name


class ConnectionManager(object):
    """
    A Connection manager. Used as a singleton.
    """
    __slots__ = ['connections']

    def __init__(self):
        self.connections = {}

    def has_single_default_connection(self):
        return True if len(self.connections) == 1 and \
            self.connections.get(CONNECTION_DEFAULT_NAME) else False

    def get(self, name=None):
        name = resolve_connection_name(name)
        try:
            return self.connections[name]()
        except KeyError:
            raise InvalidPipeline('%s is not configured' % name)

    def connect(self, pipeline_method, name=None):
        """
        Low level logic to bind a callable method to a name.
        Don't call this directly unless you know what you are doing.
        :param pipeline_method: callable
        :param name: str optional
        :return: None
        """
        name = resolve_connection_name(name)
        new_pool = pipeline_method().connection_pool
        try:
            if self.get(name).connection_pool != new_pool:
                raise AlreadyConnected("can't change connection for %s" % name)
        except InvalidPipeline:
            pass

        self.connections[name] = pipeline_method

    def connect_redis(self, redis_client, name=None,
                      transaction=True, shard_hint=None):
        """
        Store the redis connection in our connector instance.
        We grab the connection pool from the redis object
        and inject it into StrictPipeline.
        That way it doesn't matter if you pass in Redis or StrictRedis.

        :param redis_client: redis.StrictRedis() or redis.Redis()
        :param name: identifier for the connection, optional
        :param transaction:
        :param shard_hint:
        :return: None
        """
        connection_pool = redis_client.connection_pool
        response_callbacks = redis_client.response_callbacks

        if connection_pool.connection_kwargs.get('decode_responses', False):
            raise InvalidPipeline('decode_responses set to True')

        def pipeline_method():
            """
            A closure wrapping the pipeline.
            :return:
            """
            return StrictPipeline(
                connection_pool=connection_pool,
                response_callbacks=response_callbacks,
                transaction=transaction,
                shard_hint=shard_hint
            )

        # set up the connection.
        self.connect(pipeline_method=pipeline_method, name=name)

    def disconnect(self, name=None):
        """
        remove a connection by name.
        If no name is passed in, it assumes default.
        :param name:
        :return:
        """
        name = resolve_connection_name(name)
        try:
            del self.connections[name]
        except KeyError:
            pass

    def reset(self):
        self.connections = {}


connector = ConnectionManager()
connect_redis = connector.connect_redis
connect = connector.connect
disconnect = connector.disconnect
reset = connector.reset
