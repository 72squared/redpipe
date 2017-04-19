RedPipe
=======
*Making Redis pipelines easier to use in python.*

|BuildStatus| |CoverageStatus| |Version| |Python|

This project is in an *alpha* state so the interface may change.

The code is well tested and rapidly stabilizing.
Check back soon.

Requirements
------------

The redislite module requires Python 2.7 or higher.


Installation
------------

To install, use pip:

.. code-block::

    $ pip install redpipe

or from source:

.. code-block::

    $ python setup.py install


Setup
-----
To use redpipe, You need to bind your redis client instance to *RedPipe*.
Use the standard `redis-py <https://redis-py.readthedocs.io/en/latest/#>`_ client.

.. code-block:: python

    client = redis.Redis()
    redpipe.connect_redis(client)

You only need to do this setup once during application bootstrapping.

This example just sets one connection up as a default, since that is the most common case.
But you can connect multiple redis connections to *RedPipe*.
You can use `redis.StrictRedis` if you want too.
It doesn't matter.
Whatever you use normally in your application.
The goal is to reuse your application's existing redis connection.
RedPipe can be used to do everything in your application or you can just use it in certain spots.

More on this later.


Getting Started
---------------
Using redpipe is really easy.
We can pipeline multiple calls to redis and assign the results to variables.


.. code-block:: python

    with redpipe.pipeline() as pipe:
        foo = pipe.incr('foo')
        bar = pipe.incr('bar)
        pipe.execute()
    print([foo.result, bar.result])


*RedPipe* allocates a pipeline object.
Then we increment a few keys on the pipeline object.
The code looks mostly like the code you might write with redis-py pipelines.
The methods you call on the pipeline object are the same.
But, notice that each `incr` call immediately gets a reference object back in return from each call.

The references (in this case `foo` and `bar`) are empty until the pipeline executes.
Once we complete the `execute()` call we can consume the pipeline results.

This has far reaching implications.

Don't understand? Read on!

Problem Statement
-----------------
Why do I need this? Redis is really fast.

Despite this, if you have to make many calls to redis, application latency can be terrible.
This is because each command needs to make another trip across the network.
If that round trip time is one millisecond, that doesn't seem too terrible.
But if you have dozens or hundreds of redis commands, this adds up quickly.
`Redis pipelining <https://redis.io/topics/pipelining>`_ can dramatically reduce the number of network round trips.
It boxcars a bunch of commands together over the wire.
When running 50 commands against *Redis*, instead of 50 network round trips in serial order, do 1.

.. code:: python

    # this is pure redis-py code, not using redpipe here
    client = redis.Redis()
    with client.pipeline() as pipe:
        for i in range(0, 50):
            pipe.incr('foo%d' % i)

        # the one network round trip happens here.
        results = pipe.execute()

That's a 50x improvement in application latency.
And you don't need *RedPipe* to do this. It's built into *redis-py* and almost every other redis client.
But the results aren't available until you execute the pipeline.

In the example above, consuming the results on pipe execute is pretty easy.
All of the results are uniform and predictable from a loop. but what if they aren't?

Here's another example of how pipelining is usually done.

.. code:: python

    # redis-py code example, not redpipe!
    client = redis.StrictRedis()
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
    result pipe.execute()[0]

Although the calls look almost the same, the way you fetch the result is very different.

Bottom line, it's really inconvenient to use pipelines in *python*.
And it is especially inconvenient when trying to create modular and reusable components.


Solution
--------
*RedPipe* gives you the tools to break up pipelined calls into modular reusable components.

The first step is to make the commands return a reference to the data before execute happens.
The `DeferredResult` object gets populated with data once the pipeline executes.
This gives us the ability to create reusable building blocks.


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

    print(key1.result)
    print(key2.result)

Or I can call the function all by itself without passing in a pipe.

.. code:: python

    print(increment_and_expire('key3').result)

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

Auto-Commit
-----------

Iterating on our earlier example, here's another example:

.. code-block:: python

    def incr_expire(key, secs, pipe=None):
        with redpipe.pipeline(pipe=pipe, autocommit=True) as pipe:
            res = pipe.incr('foo')
            pipe.expire(key, secs)
            return res

    print(incr_expire('foo', 30))

Notice we are using the `with` control-flow structure block.
As you leave the block, it triggers the `__exit__` method on the pipe object.
If the autocommit flag was set, the method verifies no exception was thrown and executes the pipeline. If no autocommit flag is set, you must call `pipe.execute()` explicitly.


Callbacks
---------

What if we want to be able to combine the results of multiple operations inside a function?
We need some way to wait until the pipeline executes and then combine the results.
Callbacks to the rescue!

Let me show you what I mean:

.. code:: python

    def increment_keys(keys, pipe=None):
        ref = redpipe.DeferredResult()
        with redpipe.pipeline(pipe, autocommit=True) as pipe:
            results = [pipe.incr(key) for key in keys]
            def cb():
                ref.set(sum([r.result for r in results]))
            pipe.on_execute(cb)
        return ref

    # now get the value on 100 keys
    print(increment_keys(["key%d" % i for i in range(0, 100)]).result)

We didn't pass in a pipeline to the function.
It pipelines internally.
So if we are just calling the function one time, no need to pass in a pipeline.
But if we need to call it multiple times or in a loop, we can pass a pipeline in.

.. code:: python

    with redpipe.pipeline(autocommit=True) as pipe:
        first = increment_keys(["key%d" % i for i in range(0, 100)], pipe=pipe)
        second = increment_keys(["key%d" % i for i in range(100, 200)], pipe=pipe)

    print(first.result)
    print(second.result)



The pipeline context knows how to nest these operations.
As each child context completes it passes its commands and callbacks up a level.
The top pipeline context executes the functions and callbacks, creating the final result.

Named Connections
--------------------
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


Redis Cluster Support
---------------------
RedPipe supports Redis Cluster.

.. code:: python

    import rediscluster
    import redpipe
    redpipe.connect(rediscluster.StrictRedisCluster().pipeline)

This interface is still a little rough.
I hope to get better patterns around this soon.


Working with Keyspaces
----------------------
Usually when working with *Redis*, developers group a collection of keys that are similar under a keyspace.
Use a key pattern with a prefix and curly braces around the unique identifier for that record.
For example, for a list of followers for user ids `1` and `2`, I might have keys `F{1}` and `F{2}`.
*RedPipe* gives you a way to easily manipulate these keyspaces.
Here's an example of a sorted set:

.. code:: python

    class Followers(redpipe.SortedSet):
        _keyspace = 'F'
        _connection = 'default'

    with redpipe.pipeline(name='default') as pipe:
        f1 = Followers('1', pipe=pipe)
        f2 = Followers('2', pipe=pipe)
        f1.zadd('a', score=1)
        f2.zadd('a', score=2)
        f1_members = f1.zrange(0, -1)
        f2_members = f2.zrange(0, -1)
        pipe.execute()

    print(f1_members.result)
    print(f2_members.result)

We can specify what named connection we want to use with the `_connection` variable.
Or you can omit it if you are using just one default connection to redis.

All of the `redis-py` sorted set functions are exposed on the `Followers` class.
In a similar way, we support the other *Redis* primitives:

    * strings
    * sets
    * lists
    * hashes
    * sorted sets

Models
------
It is convenient to store records of data in Hashes in redis.
But hashes only represent string key-value pairs.
We need a way to type-cast variables in Redis hash fields.
That's where `redpipe.Model` comes in.

.. code:: python

    # assume we already set up our connection
    from time import time

    # set up a model object.
    class User(redpipe.Model):
        _keyspace = 'U'
        _fields = {
            'name': redpipe.TextField,
            'last_name': redpipe.TextField,
            'last_seen': redpipe.IntegerField,
            'admin': redpipe.BooleanField,
        }

        @property
        def user_id(self):
            return self.key


You can see we defined a few fields and gave them types that we can use in python.
The fields will perform basic data validation on the input and correctly serialize and deserialize from a *Redis* hash key.
Now, let's use the model.

.. code:: python

    with redpipe.pipeline(autocommit=True) as pipe:
        # create a few users
        u1 = User('1', name='Bob', last_seen=int(time()), pipe=pipe)
        u2 = User('2', name='Jill', last_seen=int(time()), pipe=pipe)

    print("first batch: %s" % [dict(u1), dict(u2)])

When we exit the context, all the models are saved to *Redis* in one pipeline operation.
Let's read those two users we created and modify them.

.. code:: python

    with redpipe.pipeline(autocommit=True) as pipe:
        users = [User('1', pipe=pipe), User('2', pipe=pipe)]
        users[0].save(name='Bobby', last_seen=int(time()), pipe=pipe)

    print("second batch: %s" % [dict(u1), dict(u2)])

When you pass just the key into the object it knows to read from the database rather than write.

Model Core
----------
Because the model is based on a `redpipe.Hash` object, you can access this if you need to extend the functionality of your model.
From our earlier `User` model example:

.. code:: python

    username = User.core('1').hget('name').result

More on this later.

.. |BuildStatus| image:: https://travis-ci.org/72squared/redpipe.svg?branch=master
    :target: https://travis-ci.org/72squared/redpipe

.. |CoverageStatus| image:: https://coveralls.io/repos/github/72squared/redpipe/badge.svg?branch=master
    :target: https://coveralls.io/github/72squared/redpipe?branch=master

.. |Version| image:: https://badge.fury.io/py/redpipe.svg
    :target: https://badge.fury.io/py/redpipe

.. |Python| image:: https://img.shields.io/badge/python-2.7,3.4,pypy-blue.svg
    :target:  https://pypi.python.org/pypi/redpipe/
