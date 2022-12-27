Rationale
=========

Why do I need this?
-------------------
Redis is really fast.
If you only use redis on your laptop over a unix domain socket, go away.
You probably do not need to think about pipelining.

But in production scenarios, redis is usually running on another machine.
That means the client needs to talk to redis server over a network.
If you are in AWS Ec2, there's a good chance your redis server is in a different availablity zone.

Your application latency is determined by the speed of your network.
If you make many calls to redis to satisfy a single request, application latency can be terrible.

Each command needs to make another trip across the network.
If your network round trip time is one millisecond, that doesn't seem too terrible.
But if you have dozens or hundreds of redis commands, this adds up quickly.

Pipelining is Cool, But ...
---------------------------
`Redis pipelining <https://redis.io/topics/pipelining>`_ can dramatically reduce the number of network round trips.
It boxcars a bunch of commands together over the wire.
When running 50 commands against *Redis*, instead of 50 network round trips in serial order, boxcar them all together in one..

.. code:: python

    # this is pure redis-py code, not using redpipe here
    client = redis.Redis()
    with client.pipeline() as pipe:
        for i in range(0, 50):
            pipe.incr('foo%d' % i)

        # the one network round trip happens here.
        results = pipe.execute()

That's a **BIG** improvement in application latency.
And you don't need *RedPipe* to do this. It's built into *redis-py* and almost every other redis client.

Pipelining is Hard to do
------------------------
Here's the catch ... *the results aren't available until after you execute the pipeline*.

In the example above, consuming the results on pipe execute is pretty easy.
All of the results are uniform and predictable from a loop. but what if they aren't?

Here's an example of pipelining heterogenous commands.

.. code:: python

    # redis-py code example, not redpipe!
    client = redis.Redis()
    with client.pipeline() as pipe:
        pipe.incr('key1')
        pipe.expire('key1', 60)
        pipe.incrby('key2', '3')
        pipe.expire('key2', 60)
        key1, expire_key1, key2, expire_key2 = pipe.execute()

See how the results are separated from the action we want to perform?
This example is a bit contrived but it illustrates a point.
We have to be careful the results from the pipeline match up with the invocation order.

And what if we want to create a reusable function that can be pipelined?

Here's what I'd like to be able to do:

.. code:: python

    def increment_and_expire(key, num, expire, pipe):
        pipe.incrby(key, num)
        pipe.expire(expire)
        # return result of incrby operation
        # HOW????

I want to return the result of the `pipe.incrby` call from the function.
But the result isn't available until `pipe.execute`.
This happens outside the scope of the function.
And the caller of the function doesn't know how many pipeline calls were invoked.
So grabbing the correct value from pipe.execute() is tricky.

Also, consider the difference between the direct and pipeline interfaces in *redis-py*:

.. code:: python

    # redis-py non-pipelined example
    result = client.incr('key1')

vs.

.. code:: python

    # redis-py pipelined example
    pipe = client.pipeline()
    pipe.incr('key1')
    result = pipe.execute()[0]

Although the calls look almost the same, the way you fetch the result is very different.

Bottom line, it's inconvenient to use pipelines in *python*.
And it is especially inconvenient when trying to create modular and reusable components.


How RedPipe Makes Things Easier
-------------------------------
*RedPipe* makes things easier by first making it harder.
It's a paradigm shift.
You ready?
Here it comes.

*All redis calls are pipelined.*

On the surface this seems unnecessary.
But stick with me for a few minutes.
It will unlock the tools to break up pipelined calls into modular reusable components.


The first step is to make the commands return a reference to the data immediately.
We'll call this reference object a `Future`.
The `redpipe.Future` object gets populated with data once the pipeline executes.

That makes the code look very much like a non-pipelined call.
You invoke the redis-py method and you get a response back from that call.
The response is a `redpipe.Future` object, but you don't ever need to think about that.

Once the pipeline executes, the `Future` behaves just like the underlying result.

*RedPipe* embraces the spirit of `duck-typing <https://en.wikipedia.org/wiki/Duck_typing#In_Python>`_.

You can iterate on a Future if the result is a list.
Add or subtract from it if it is an int.
Print it out like a string.
In short, you should be able to use it interchangeably with the underlying `future.result` field.

This gives us the ability to create reusable building blocks.

How, wait what??

Okay, keep reading.
I'll explain.


Reusable Building Blocks
------------------------
here's how *RedPipe* allows me to do what I wanted to do above.

.. code:: python

    def increment_and_expire(key, num=1, expire=60, pipe=None):
        pipe = redpipe.pipeline(pipe)
        ref = pipe.incrby(key, num)
        pipe.expire(key, expire)
        pipe.execute()
        return ref

Now we have a reusable function!
`redpipe.pipeline` can give us a pipeline if no pipeline is passed into the function.
Or it wraps the one passed in.
Let's invoke our function!

.. code:: python

    with redpipe.pipeline() as pipe:
        key1 = increment_and_expire('key1', pipe=pipe)
        key2 = increment_and_expire('key2', pipe=pipe)
        pipe.execute()

    print(key1)
    print(key2)

Or I can call the function all by itself without passing in a pipe.

.. code:: python

    print(increment_and_expire('key3'))

The function will always pipeline the *incrby* and *expire* commands together.

When we pass in one pipeline() into another, it creates a nested structure.
When we pass in a pipeline to our function, it will combine with the other calls above it too!
So you could pipeline a hundred of calls without any more complexity:

.. code:: python

    with redpipe.pipeline() as pipe:
        results = [increment_and_expire('key%d' % i, pipe=pipe) for i in range(0, 100)]
        pipe.execute()
    print(results)

We have sent 200 redis commands with only 1 network round-trip. Pretty cool, eh?
This only scratches the surface of what we can do.
