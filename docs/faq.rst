FAQ
===

Q: Why am I getting InvalidPipeline?
------------------------------------
Have you configured your connection yet?

.. code-block:: python

    redis_client = redis.Redis()
    redpipe.connect_redis(redis_client)

This will pass the redis connection to redpipe.


Q: I used `decode_responses` in redis and got an error in redpipe. WTF?
-----------------------------------------------------------------------
Short answer: I raised an exception on purpose.

I decided to be very opinionated. I don't want you to do this.

When talking to redis, you don't know for sure what you are getting.
It might be binary data. It might not.

We wait to decode responses until we are down a layer when we know the data type
of the keyspace we are using and the fields. That allows us to reuse the same connection
to write and read binary data and still decode responses that should be.

If you feel this is wrong, let's chat.
I'm open to discussion.

Raise it in `issues <https://github.com/72squared/redpipe/issues>`_.


Q: Why name it RedPipe? That's dumb.
------------------------------------
Yeah.

I'm not the best at naming things.

*Red* is short for *Redis*.
*Pipe* is short for *Pipelining*.
Put the two together.
*RedPipe.*
That's the sum total of my thought process in naming my module.

Plus, no one had used it yet in *PyPi*.

:)
