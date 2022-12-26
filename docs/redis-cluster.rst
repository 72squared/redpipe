Open Source Redis Cluster Support
=================================

*RedPipe* supports Redis Cluster.
This became even easier now that redis-py supports redis cluster natively.

.. code:: python

    import redpipe

    r = redis.RedisCluster.from_url('redis://127.0.0.1:7000')

    redpipe.connect_redis(r, name='my-cluster')

The reason you can do this is because **RedPipe** wraps the interface.

If it quacks like a duck ...
