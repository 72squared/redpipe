Open Source Redis Cluster Support
=================================

*RedPipe* supports Redis Cluster.

.. code:: python

    import rediscluster
    import redpipe

    r = rediscluster.RedisCluster(
        startup_nodes=[{'host': '0', 'port': 7000}],
    )

    redpipe.connect_redis(r, name='my-cluster')

The reason you can do this is because **RedPipe** wraps the interface.

If it quacks like a duck ...

**RedPipe** can support both the strict and normal interfaces.
Because it is a wrapper, the commands buffer just as you send them.
So if you wrap a RedisCluster, the commands will be sent through as strict commands.
If you wrap `RedisCluster` it follows that interface.

When you get to `Keyspaces`, **RedPipe** is more opinionated.
You can use RedisCluster.
But it will present an interface more like the non-strict version.
