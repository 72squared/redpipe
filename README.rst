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

Getting Started
---------------
Using redpipe is really easy.
We can pipeline multiple calls to redis and assign the results to variables.

.. code-block:: python

    import redpipe
    import redis
    client = redis.Redis()
    pipe = redpipe.Pipeline(client.pipeline())
    foo = pipe.incr('foo')
    bar = pipe.incr('bar)
    pipe.execute()
    print([foo.result, bar.result])

Here is an equivalent block of code with some more sugar:

.. code-block:: python

    import redpipe
    import redis

    redpipe.connect_redis(redis.Redis())
    with redpipe.PipelineContext() as pipe:
        foo = pipe.incr('foo')
        bar = pipe.incr('bar)

    print([foo.result, bar.result])

The `PipelineContext` object is very powerful.
We'll cover more of what it can do later.
For now, notice we are using the `with` control-flow structure block.
As you leave the block, it triggers the `__exit__` method on the `PipelineContext`.
This method verifies no exception was thrown and executes the pipeline.

We can safely consume the results after leaving the context block.

In both cases, we perform an operation on a pipeline object and get a reference back in return.
The reference is empty until the pipeline executes.

Don't understand? Read on!

Why RedPipe?
------------
`Redis pipelining <https://redis.io/topics/pipelining>`_ is really powerful.
It can dramatically reduce the number of network round trips.
Each individual call to `Redis` is really fast.
But if you have to access a bunch of different keys in your application, it adds up.

Pipelining can reduce the amount of round trips your application needs to make.
But the results aren't available until you execute the pipeline.
This makes it inconvenient to use pipelines in *python*.
And it is especially inconvenient when trying to create modular and reusable components.

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

*RedPipe* solves all that.
It returns a *DeferredResult* object from each method invocation in *Redis* pipeline.
The DeferredResult object gets populated with data once the pipeline executes.
This gives us the ability to create reusable building blocks.


here's how *RedPipe* allows me to do what I wanted to do above.

.. code:: python

    def increment_and_expire(key, num=1, expire=60, pipe=None):
        with redpipe.PipelineContext(pipe) as pipe:
            ref = pipe.incrby(key, num)
            pipe.expire(key, expire)
            return ref

Now we have a reusable function!
`PipelineContext` can give us a pipeline if none is passed into the function.
Or it wraps the one passed in.
The nested PipelineContext() inside the function combines with the one passed in.

.. code:: python

    with redpipe.PipelineContext() as pipe:
        key1 = increment_and_expire('key1', pipe=pipe)
        key2 = increment_and_expire('key2', pipe=pipe)

    print(key1.result)
    print(key2.result)

Or I can call the function all by itself without passing in a pipe.

.. code:: python

    print(increment_and_expire('key3').result)

The function will always pipeline the *incrby* and *expire* commands together.

When we pass in another PipelineContext() into another PipelineContext() it creates a nested structure.
When we pass in a pipeline to our function, it will combine with the other calls above it too!
So you could pipeline a hundred of calls without any more complexity:

.. code:: python

    with redpipe.PipelineContext() as pipe:
        results = [increment_and_expire('key%d' % i, pipe=pipe) for i in range(0, 100)]

We have sent 200 redis commands with only 1 network round-trip. Pretty cool, eh?
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
        with redpipe.PipelineContext(pipe) as pipe:
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

    redpipe.connect_redis(redis.StrictRedis(port=6379), name='users')
    redpipe.connect_redis(redis.StrictRedis(port=6380), name='messages')
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
    redpipe.connect(rediscluster.StrictRedisCluster().pipeline)

This interface is still a little rough.
I hope to get better patterns around this soon.


Working with Keyspaces
----------------------
Usually when working with *Redis*, people will group a collection of keys that are similar under a namespace.
They use a key pattern with a prefix and curly braces around the unique identifier for that record.
For example, for a list of followers for user ids `1` and `2`, I might have keys `F{1}` and `F{2}`.
*RedPipe* gives you a way to easily manipulate these keyspaces.
Here's an example of a sorted set:

.. code:: python

    class Followers(redpipe.SortedSet):
        _keyspace = 'F'
        _context = 'default'

    with redpipe.PipelineContext(name='default') as pipe:
        f1 = Followers('1', pipe=pipe)
        f2 = Followers('2', pipe=pipe)
        f1.zadd('a', score=1)
        f2.zadd('a', score=2)
        f1_members = f1.zrange(0, -1)
        f2_members = f2.zrange(0, -1)

    print(f1_members.result)
    print(f2_members.result)

Note how we can specify what named context we want to use with the `_context` variable.
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

    with redpipe.PipelineContext() as pipe:
        # create a few users
        u1 = User('1', name='Bob', last_seen=int(time()), pipe=pipe)
        u2 = User('2', name='Jill', last_seen=int(time()), pipe=pipe)

    print("first batch: %s" % [dict(u1), dict(u2)])

When we exit the context, all the models are saved to *Redis* in one pipeline operation.
Let's read those two users we created and modify them.

.. code:: python

    with redpipe.PipelineContext() as pipe:
        users = [User('1', pipe=pipe), User('2', pipe=pipe)]
        users[0].save(name='Bobby', last_seen=int(time()), pipe=pipe)

    print("second batch: %s" % [dict(u1), dict(u2)])

When you pass just the key into the object it knows to read from the database rather than write.

The interface for models is simple but powerful.


.. |BuildStatus| image:: https://travis-ci.org/72squared/redpipe.svg?branch=master
    :target: https://travis-ci.org/72squared/redpipe

.. |CoverageStatus| image:: https://coveralls.io/repos/github/72squared/redpipe/badge.svg?branch=master
    :target: https://coveralls.io/github/72squared/redpipe?branch=master

.. |Version| image:: https://badge.fury.io/py/redpipe.svg
    :target: https://badge.fury.io/py/redpipe

.. |Python| image:: https://img.shields.io/badge/python-2.7,3.4,pypy-blue.svg
    :target:  https://pypi.python.org/pypi/redpipe/
