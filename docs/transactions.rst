Transactions
============
When you talk aboout redis pipelining, most people conflate it with `transactions <https://redis.io/topics/transactions>`_.
In fact, the `redis-py` library conflates it by making a `transaction` flag you pass into the pipeline object.
There has been a lot of effort to make Redis behave in a transactional way.

This is not a goal for **RedPipe**.

 **RedPipe** was written to improve network i/o.

Most of the concepts for **RedPipe** came from a project that uses Redis Cluster.
It's not practical or supported to use transactions there.
Any kind of atomic multi-step operation is limited to a single key, and is best accomplished with a LUA script.

I haven't disallowed transactions.
But I'm not going out of my way to try to support it either.

You can turn transactions on or off in setting up your connection.

.. code-block:: python

    client = redis.StrictRedis()
    redpipe.connect_redis(client, transaction=False)

I welcome discussion.
If this is a pain point for you, `let me know <https://github.com/72squared/redpipe/issues>`.

