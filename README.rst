RedPipe
=======

*Making Redis pipelines easier to use in python.*

*Redis* pipelining is `powerful <https://redis.io/topics/pipelining>`_.
But the results aren't available until you execute the pipeline.
It's inconvenient to use pipelines in python.
This is especially true when trying to create modular and reusable components.


|Build Status| |Coverage Status|

The Problem
-----------
Take a look at how pipelining is normally done.

.. code:: python

    import redis
    client = redis.StrictRedis()
    pipe = client.pipeline()
    pipe.incr('key1')
    pipe.expire('key1', 60)
    pipe.incrby('key2', '3')
    pipe.expire('key2', 60)
    key1, expire_key1, key2, expire_key2 = pipe.execute()

See how the results are decoupled from the action we want to perform?
This is a pretty this silly example.
But we still have to be careful the results from the pipeline match up with the invocation order.

And what if we want to create a reusable function that can be pipelined?

Here's what I'd like to be able to do:

.. code:: python

    def increment_and_expire(key, num, expire, pipe):
        pipe.incrby(key, num)
        pipe.expire(expire)
        # return result of incrby operation
        # HOW????

The problem is that I don't have a way to access the result of that operation.

Solution
--------
*RedPipe* solves all that.
It returns a *DeferredResult* object from each method invocation in *Redis* pipeline.
The DeferredResult object gets populated with data once the pipeline executes.
This gives us the ability to create reusable building blocks.

.. code:: python

    import redis
    import redpipe

    # initialize our connection
    redpipe.connect(redis.StrictRedis())

    # here's the function I couldn't do above.
    def increment_and_expire(key, num=1, expire=60, pipe=None):
        with redpipe.PipelineContext(pipe) as pipe:
            ref = pipe.incrby(key, num)
            pipe.expire(expire)
            return ref

    # now we can call our reusable function
    with redpipe.PipelineContext() as pipe:
        key1 = increment_and_expire('key1', pipe=pipe)
        key2 = increment_and_expire('key2', pipe=pipe)

    # now that I've exited the context block, I can consume the results.
    print(key1.result)
    print(key2.result)

    # and i can do the function all by itself without passing in a pipe
    print(increment_and_expire('key3').result)

Now our function will always pipeline the *incrby* and *expire* commands together.
And, if we pass in a pipeline, it will combine the other calls too!
So you could pipeline a hundred of calls without any more complexity:

.. code:: python

    with redpipe.PipelineContext() as pipe:
        results = [increment_and_expire('key%d' % i, pipe=pipe) for i in range(0, 100)]


This only scratches the surface of what we can do.

Callbacks
---------

What if we want to be able to combine the results of multiple operations inside a function?
We need some way to wait until the pipeline executes and then combine the results.
Callbacks to the rescue!

Let me show you what I mean:

.. code:: python

    def increment_keys(keys, pipe=None):
        ref = redpipe.DeferredResult()
        with redpipe.Context(pipe) as pipe:
            results = [pipe.incr(key) for key in keys]
            def cb():
                ref.set(sum([r.result for r in results]))
            pipe.on_execute(cb)
        return ref

    # now get the value on 100 keys
    print(increment_keys(["key%d" % i for i in range(0, 100)]).result)

Notice how we didn't pass in a pipeline.
But we could have.

.. code:: python

    with redpipe.PipelineContext() as pipe:
        first = increment_keys(["key%d" % i for i in range(0, 100)], pipe=pipe)
        second = increment_keys(["key%d" % i for i in range(100, 200)], pipe=pipe)

    print(first.result)
    print(second.result)



The pipeline context knows how to nest these operations.
As each child context completes it passes its commands and callbacks up a level.
The top pipeline context executes the functions and callbacks, creating the final result.

Multiple Connections
--------------------
So far the examples I've shown have assumed only one connection to `Redis`.
But what if you need to talk to multiple backends?
*RedPipe* allows you to set up different connections and then refer to them:

.. code:: python

    redpipe.connect(redis.StrictRedis(port=6379), name='users')
    redpipe.connect(redis.StrictRedis(port=6380), name='messages')
    with redpipe.PipelineContext(name='users') as users:
        users.hset('u{1}', 'name', 'joe')

    with redpipe.PipelineContext(name='messages') as messages:
        messages.hset('m{1}', 'body', 'hi there')


Redis Cluster Support
---------------------
RedPipe supports Redis Cluster.

.. code:: python

    import rediscluster
    import redpipe
    redpipe.connect_redis_pipeline(rediscluster.StrictRedisCluster().pipeline)

This interface is still a little rough.
I hope to get better patterns around this soon.


Working with Keyspaces
----------------------
Usually when working with *Redis*, people will group a collection of keys that are similar under a namespace.
They use a key pattern with a prefix and curly braces around the unique identifier for that record.
For example, for users 1 and 2, I might have keys `U{1}` and `U{2}`.
*RedPipe* gives you a way to easily manipulate these keyspaces.
Here's an example of a sorted set:

.. code:: python

    class Followers(redpipe.SortedSet):
        _namespace = 'F'
        _db = 'default'

    with redpipe.PipelineContext('default') as pipe:
        f1 = Followers('1', pipe=pipe)
        f2 = Followers('2', pipe=pipe)
        f1.zadd('a', score=1)
        f2.zadd('a', score=2)
        f1_members = f1.zrange(0, -1)
        f2_members = f2.zrange(0, -1)
    print(f1_members.result)
    print(f2_members.result)

All of the sorted set functions are exposed on the Followers class.
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

    import redpipe
    import redis
    from time import time

    # configure redpipe.
    # only need to do this once in your application.
    redpipe.connect(redis.StrictRedis())

    # set up a model object.
    class User(redpipe.Model):
        _namespace = 'U'
        _fields = {
            'name': redpipe.TextField,
            'last_name': redpipe.TextField,
            'last_seen': redpipe.IntegerField,
            'admin': redpipe.BooleanField,
        }

        @property
        def user_id(self):
            return self.key


    # now let's use the model.
    with redpipe.PipelineContext() as pipe:
        # create a few users
        u1 = User('1', name='Bob', last_seen=int(time()), pipe=pipe)
        u2 = User('2', name='Jill', last_seen=int(time()), pipe=pipe)

    print("first batch: %s" % [dict(u1), dict(u2)])

    # when we exit the context, all the models are saved to redis
    # in one pipeline operation.
    # now let's read those two users we created and modify them
    with redpipe.PipelineContext() as pipe:
        users = [User('1', pipe=pipe), User('2', pipe=pipe)]
        users[0].save(name='Bobby', last_seen=int(time()), pipe=pipe)

    print("second batch: %s" % [dict(u1), dict(u2)])

.. |Build Status| image:: https://travis-ci.org/72squared/redpipe.svg?branch=master
   :target: https://travis-ci.org/72squared/redpipe

.. |Coverage Status| image:: https://coveralls.io/repos/github/72squared/redpipe/badge.svg?branch=master
   :target: https://coveralls.io/github/72squared/redpipe?branch=master
