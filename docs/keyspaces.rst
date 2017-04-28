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

    print(f1_members)
    print(f2_members)

We can specify what named connection we want to use with the `_connection` variable.
Or you can omit it if you are using just one default connection to redis.

All of the `redis-py` sorted set functions are exposed on the `Followers` class.
In a similar way, we support the other *Redis* primitives:

    * strings
    * sets
    * lists
    * hashes
    * sorted sets

Fields in Hashes
----------------
Often you want to store data in Hashes that maps to a particular data type.
For example, a boolean flag, an integer, or a float.
Redis stores all the values as byte strings and doesn't interpret.
We can set up explicit mappings for these data types in `redpipe.Hash`.
This is not required but it makes life easier.

.. code:: python

    class User(redpipe.Hash):
        _keyspace = 'U'
        _fields = {
            'first_name': redpipe.StringField,
            'last_name': redpipe.StringField,
            'admin': redpipe.BooleanField,
            'last_seen': redpipe.FloatField,
        }


You can see we defined a few fields and gave them types that we can use in python.
The fields will perform basic data validation on the input and correctly serialize and deserialize from a *Redis* hash key.

.. code:: python

    with redpipe.pipeline(autocommit=True) as pipe:
        u = User('1', pipe=pipe)
        data = {
            'first_name': 'Fred',
            'last_name': 'Flitstone',
            'admin': True,
            'last_seen': time.time(),
        }
        u.hmset(data)
        ref = u.hgetall()

    assert(ref == data)

You can see this allows us to set booleans, ints and other data types into the hash and get the same values back.

