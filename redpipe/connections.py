# -*- coding: utf-8 -*-
"""
Bind instances of the redis-py or redis-py-cluster client to redpipe.
Assign named connections to be able to talk to multiple redis servers in your
project.

The ConnectionManager is a singleton class.

These functions are all you will need to call from your code:

* connect_redis
* connect_rediscluster
* disconnect
* reset

Everything else is for internal use.
"""

from redis.client import StrictPipeline
from .exceptions import AlreadyConnected, InvalidPipeline

__all__ = [
    'connect_redis',
    'connect_rediscluster',
    'disconnect',
    'reset'
]


class ConnectionManager(object):
    """
    A Connection manager. Used as a singleton.

    Don't invoke methods on this class directly.
    Instead use the convenience methods defined in this module:

    * connect_redis
    * connect_redis_cluster
    * disconnect
    * reset

    """
    connections = {}

    DEFAULT_NAME = 'default'

    @classmethod
    def resolve(cls, name=None):
        """
        Utility method for resolving the connection name

        :param name: str or None
        :return: str
        """
        return cls.DEFAULT_NAME if name is None else name

    @classmethod
    def get(cls, name=None):
        """
        Get a new redis-py pipeline object or similar object.
        Called by the redpipe.pipelines module.
        Don't call this directly.

        :param name: str
        :return: callable implementing the redis-py pipeline interface.
        """
        name = cls.resolve(name)
        try:
            return cls.connections[name]()
        except KeyError:
            raise InvalidPipeline('%s is not configured' % name)

    @classmethod
    def connect(cls, pipeline_method, name=None):
        """
        Low level logic to bind a callable method to a name.
        Don't call this directly unless you know what you are doing.

        :param pipeline_method: callable
        :param name: str optional
        :return: None
        """
        name = cls.resolve(name)
        new_pool = pipeline_method().connection_pool
        try:
            if cls.get(name).connection_pool != new_pool:
                raise AlreadyConnected("can't change connection for %s" % name)
        except InvalidPipeline:
            pass

        cls.connections[name] = pipeline_method

    @classmethod
    def connect_redis(cls, redis_client, name=None,
                      transaction=False, shard_hint=None):
        """
        Store the redis connection in our connector instance.

        Do this during your application bootstrapping.

        We grab the connection pool from the redis object
        and inject it into StrictPipeline.
        That way it doesn't matter if you pass in Redis or StrictRedis.

        The transaction flag is a boolean value we hold on to and
        pass to the invocation of something equivalent to:

        .. code-block:: python

            redis_client.pipeline(transaction=transation)

        Unlike redis-py, this flag defaults to False.
        You can configure it to always use the MULTI/EXEC flags,
        but I don't see much point.

        If you need transactional support I recommend using a LUA script.

        **RedPipe** is about improving network round-trip efficiency.

        :param redis_client: redis.StrictRedis() or redis.Redis()
        :param name: identifier for the connection, optional
        :param transaction: bool, defaults to False
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

            :return: redis.StrictPipeline()
            """
            return StrictPipeline(
                connection_pool=connection_pool,
                response_callbacks=response_callbacks,
                transaction=transaction,
                shard_hint=shard_hint
            )

        # set up the connection.
        cls.connect(pipeline_method=pipeline_method, name=name)

    @classmethod
    def connect_rediscluster(cls, redis_cluster_client, name=None):
        """
        Call this during your application bootstrapping to link your instance
        of redis-py-cluster to redpipe.

        redis-py-cluster internals are messy and have changed a lot
        so I can't be quite as elegant here as I was with redis.
        You really need to pass me the StrictRedisCluster object.

        :param redis_cluster_client: rediscluster.StrictRedisCluster()
        :param name: identifier for the connection, optional
        :return: None
        """
        def pipeline_method():
            """
            A closure wrapping the pipeline.

            :return: rediscluster.StrictClusterPipeline()
            """
            return redis_cluster_client.pipeline()

        # set up the connection.
        cls.connect(pipeline_method=pipeline_method, name=name)

    @classmethod
    def disconnect(cls, name=None):
        """
        remove a connection by name.
        If no name is passed in, it assumes default.

        Useful for testing.

        :param name:
        :return:
        """
        name = cls.resolve(name)
        try:
            del cls.connections[name]
        except KeyError:
            pass

    @classmethod
    def reset(cls):
        """
        remove all connections.
        Useful for testing scenarios.

        :return: None
        """
        cls.connections = {}


def connect_redis(redis_client, name=None, transaction=True, shard_hint=None):
    """
    Connect your redis-py instance to redpipe.

    Example:

    .. code:: python

        redpipe.connect_redis(redis.StrictRedis(), name='users')


    Do this during your application bootstrapping.

    :param redis_client:
    :param name:
    :param transaction:
    :param shard_hint:
    :return:
    """
    return ConnectionManager.connect_redis(
        redis_client=redis_client, name=name, transaction=transaction,
        shard_hint=shard_hint)


def connect_rediscluster(redis_cluster_client, name=None):
    """
    Connect an instance of the redis-py-cluster client to redpipe.

    Call this during your application bootstrapping.

    Example:

    .. code:: python

        client = rediscluster.StrictRedisCluster(
            startup_nodes=[{'host': '0', 'port': 7000}]
        )
        redpipe.connect_rediscluster(client, name='users')


    """
    return ConnectionManager.connect_rediscluster(
        redis_cluster_client=redis_cluster_client,
        name=name
    )


def disconnect(name=None):
    """
    remove a connection by name.
    If no name is passed in, it assumes default.

    Useful for testing.

    :param name:
    :return: None
    """
    return ConnectionManager.disconnect(name=name)


def reset():
    """
    remove all connections.
    Useful for testing scenarios.

    :return: None
    """
    return ConnectionManager.reset()
