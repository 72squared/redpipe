Open Source Redis Cluster Support
=================================

*RedPipe* supports Redis Cluster.

.. code:: python

    import rediscluster
    import redpipe

    r = rediscluster.StrictRedisCluster(
        startup_nodes=[{'host': '0', 'port': 7000}],
    )

    redpipe.connect_rediscluster(r, name='my-cluster')


This api isn't quite as clean as the one for `connect_redis`.
I'm not able to easily instantiate the StrictRedisPipeline with only a connection pool.
It's possible in the 1.3.0 or greater version, but older versions present a challenge.
I am relying on you to always pass StrictRedisCluster.
In future versions, I may be able to correctly detect the version of `redis-py-cluster` you are using.
We'll see.
For now, this should work well enough.

If it doesn't work for your use case, you can build your own connector by passing a callable function to `redpipe.connect`.
It expects something that creates a pipeline object with a similar interface as that offered by the `redis-py` pipeline.
Use at your own risk.
