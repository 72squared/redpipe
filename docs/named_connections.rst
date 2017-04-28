Named Connections
=================

So far the examples I've shown have assumed only one connection to `Redis`.
But what if you need to talk to multiple backends?
*RedPipe* allows you to set up different connections and then refer to them:

.. code:: python

    redpipe.connect_redis(redis.StrictRedis(port=6379), name='users')
    redpipe.connect_redis(redis.StrictRedis(port=6380), name='messages')


Now I can refer to those named connections inside my functions and throughout my application.

.. code:: python

    with redpipe.pipeline(name='users', autocommit=True) as users:
        users.hset('u{1}', 'name', 'joe')

    with redpipe.pipeline(name='messages', autocommit=True) as messages:
        messages.hset('m{1}', 'body', 'hi there')

If you don't specify a name, it assumes a default connection set up like this:

.. code:: python

    redpipe.connect_redis(redis.StrictRedis(port=6379))

You can actually map the same redis connection to multiple names if you want.
This is good for aliasing names when preparing to split up data, or for testing.
