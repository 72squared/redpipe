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
