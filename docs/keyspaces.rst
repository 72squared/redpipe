Working with Keyspaces
======================
Usually when working with *Redis*, developers often group a collection of keys that are similar under a keyspace.
Use a key pattern with a prefix and curly braces around the unique identifier for that record.
For example, for a list of followers for user ids `1` and `2`, I might have keys `F{1}` and `F{2}`.

This keyspace functions as a virtual table, like what you might have in a typical RDBMS.
Except that each key is really independent.
We just use a naming convention to group them together.


Example of a Sorted Set Keyspace
--------------------------------
*RedPipe* gives you a way to easily manipulate these keyspaces.

Here's an example of a sorted set:

.. code:: python

    class Followers(redpipe.SortedSet):
        keyspace = 'F'
        connection = 'default'

    key1 = '1'
    key2 = '2'
    with redpipe.pipeline(name='default') as pipe:
        f = Followers(pipe=pipe)
        f.zadd(key1, 'a', score=1)
        f.zadd(key2, 'a', score=2)
        f1_members = f.zrange(key1, 0, -1)
        f2_members = f.zrange(key2, 0, -1)
        pipe.execute()

    print(f1_members)
    print(f2_members)

We can specify what named connection we want to use with the `connection` variable.
Or you can omit it if you are using just one default connection to redis.

You will notice the interface provided by the keyspace object `redpipe.SortedSet` looks just like `redis-py` functions.
Except it omits the name of the key. That's because the key name is already specified in the constructor.


Supported Keyspace Types
------------------------
All of the `redis-py` sorted set functions are exposed on the in the example above.

In a similar way, we support the other *Redis* primitives:

    * strings
    * sets
    * lists
    * hashes
    * sorted sets
    * hyperloglog
    * geo (in progress)

All the commands associated with each data type are exposed for each.
See the `official redis documentation`_ for more information, or refer to `redis-py`_.

Hashed Strings
--------------

Sometimes you have a set of keys that are simple key/value pairs and it makes
more sense to store all of them in a really large hash. That way you can store
all of them in one key. The overhead is much lower than storing thousands or
millions of individual keys.

But eventually you may find that you are storing hundreds of millions of these
pairs in a single hash key. In that case it makes more sense to be able to
split these up. This is what the Hashed Strings pattern does for you.

This is not really a core part of redpipe, but the pattern came up so
frequently that it made sense to include it here. And it is a relatively
small piece of code.

For now, refer to the test-case on how to use this.

More detailed documentation will be given in the future as this logic
solidifies.

Here is what a definition might look like:

.. code-block:: python

    class MyIndex(redpipe.HashedStrings):
        keyspace = 'my_index'
        connection = 'default'
        shard_count = 1000


This will namespace the shards under the keyspace `my_index{%s}`.
The string interpolated inside of this keyspace will be a string of digits
between 0 and 999.

To invoke it, you can do:

.. code-block:: python
    with redpipe.pipeline(autoexec=True) as pipe:
        i = MyIndex(pipe)
        i.set('foo123', 'test')
        result = i.get('foo123')

The result returned for key `foo123` is a string `test`.

Character Encoding in Keyspaces
-------------------------------
When you use `redpipe.pipeline()` directly, **RedPipe** disables automatic character decoding.
That's because there's no way to know how to decode responses for every single request that goes through redis.
The dump/restore commands, for example, never should automatically decode the binary data.
It's not utf-8.
And if you are pickling python objects and storing them in redis, character encoding makes no sense.

With a Keyspace, though, it's entirely appropriate to map the binary data in redis to appropriate encodings.
That's because you are defining some application

There are some defaults you can tune per keyspace that you define:

* keyparse
* valueparse

We treat these as utf-8 encoded unicode strings, controlled by the formatter `redpipe.TextField`.
There are many other data types you can use.

They control how to encode the key and the values in the redis data structures.

In addition, `redpipe.Hash` gives you additional ways to encode and decode data for each individual member of the Hash.



Fields in Hashes
----------------
Often you want to store data in Hashes that maps to a particular data type.
For example, a boolean flag, an integer, or a float.
Redis stores all the values as byte strings and doesn't interpret.
In the Keyspace, we default to treating all fields as unicode that is stored in redis as utf-8 binary strings.
If you need something different, you can set up explicit mappings for other data types in `redpipe.Hash`.
This is not required but it makes life easier.

.. code:: python

    class User(redpipe.Hash):
        keyspace = 'U'
        fields = {
            'first_name': redpipe.TextField,
            'last_name': redpipe.TextField,
            'admin': redpipe.BooleanField,
            'last_seen': redpipe.FloatField,
            'encrypted_secret': redpipe.BinaryField,
        }


You can see we defined a few fields and gave them types that we can use in python.
The fields will perform basic data validation on the input and correctly serialize and deserialize from a *Redis* hash key.

.. code:: python

    key = '1'
    with redpipe.autoexec() as pipe:
        u = User(pipe=pipe)
        data = {
            'first_name': 'Fred',
            'last_name': 'Flitstone',
            'admin': True,
            'last_seen': time.time(),
        }
        u.hmset(key, data)
        ref = u.hgetall(key)

    assert(ref == data)

You can see this allows us to set booleans, ints and other data types into the hash and get the same values back.

Data Types defined for Keyspaces
--------------------------------

Here's a list of all the different data types you can represent so far:

* BooleanField
* FloatField
* IntegerField
* TextField
* AsciiField
* BinaryField
* ListField
* DictField
* StringListField

If you don't see the one you want, you can always write your own.
It's pretty easy.
You just need an object that provides two methods:

* encode
* decode

The encode method that converts your python data structure into binary string.
And the decode method to will convert it back consistently into your original python structure.

Scanning the Keys in a Keyspace
-------------------------------
When you use the `scan` command on a keyspace, **RedPipe** automatically builds a pattern that matches the keyspace you are using.
Any additional patterns you pass in are searched for inside of that pattern.
So you should be able easily iterate through a list of all keys in the keyspace.

The scan commands don't seem to work quite right in redis-py-cluster.
I'm working with the package maintainer to try to get that squared away.



.. _official redis documentation: https://redis.io/commands
.. _redis-py: https://redis-py.readthedocs.io/en/latest/index.html#module-redis

