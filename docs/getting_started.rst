Getting Started
===============

Installation
------------

To install, use pip:

.. code-block:: bash

    pip install redpipe

or from source:

.. code-block:: bash

    python setup.py install


Connect redis-py to RedPipe
---------------------------
To use redpipe, You need to bind your redis client instance to *RedPipe*.
Use the standard `redis-py <https://redis-py.readthedocs.io/en/latest/#>`_ client.

.. code-block:: python

    client = redis.Redis()
    redpipe.connect_redis(client)

You only need to do this setup once during application bootstrapping.

This example just sets one connection up as a default, since that is the most common case.
But you can connect multiple redis connections to *RedPipe*.


You can use `StrictRedis` if you want too.
It doesn't matter.
Whatever you use normally in your application.

The goal is to reuse your application's existing redis connection.
RedPipe can be used to build your entire in your application.
Or you can use *RedPipe* along side your existing code.

More on this later.


Using RedPipe
-------------
Using *RedPipe* is easy.
We can pipeline multiple calls to redis and assign the results to variables.


.. code-block:: python

    with redpipe.pipeline() as pipe:
        foo = pipe.incr('foo')
        bar = pipe.incr('bar)
        pipe.execute()
    print([foo, bar])


*RedPipe* allocates a pipeline object.
Then we increment a few keys on the pipeline object.
The code looks mostly like the code you might write with redis-py pipelines.
The methods you call on the pipeline object are the same.
But, notice that each `incr` call immediately gets a reference object back in return from each call.
That part looks similar to how `redis-py` works without a pipeline.

The variables (in this case `foo` and `bar`) are empty until the pipeline executes.
if you try to do any operations on them beforehand, it will raise an exception.
Once we complete the `execute()` call we can consume the pipeline results.
These variables, `foo` and `bar`, behave just like the underlying result once the pipeline executes.
You can iterate of it, add it, multiply it, etc.

This has far reaching implications.

Don't understand? Read on!