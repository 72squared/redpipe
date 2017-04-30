Structs
=======

*RedPipe* does not provide you with an ORM solution.
There are many other packages out there that do that much better.
Choose *Redpipe* if you really care about optimizing your network i/o.
To do that effectively, you need to combine as many different parallel operations as possible.
ORMs often hide things like automatic unique constraints and indexes beneath the covers.
That makes it tricky to ensure you are batching those operations efficently as possible over the network.

RedPipe exposes these primitives keep the data structures simple inputs and outputs.
This allows us to construct with building blocks that can be pipelined efficiently.

However, it is nice when storing data into redis to be able to load data and persist it into a hash and still preserve the data-type of the original structure.
You could use a string key and serialize in json.
This would defeat the power of redis.

We gave `redpipe.Hash` the ability to type-cast variables stored in redis.
But we could make it more convenient to fetch and save data as objects.
That's where `redpipe.Struct` comes in.

.. code:: python

    # assume we already set up our connection
    from time import time

    # set up a struct object.
    class User(redpipe.Struct):
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


A lot of this looks very similar to what we did with `redpipe.Hash`.
That's because struct is built on top of the Hash object.
The struct object is all about syntactic sugar to easily access variables and
be able to manipulate them in a more object oriented manner.

.. code:: python

    with redpipe.pipeline(autocommit=True) as pipe:
        # create a few users
        u1 = User('1', name='Bob', last_seen=int(time()), pipe=pipe)
        u2 = User('2', name='Jill', last_seen=int(time()), pipe=pipe)


    print("first batch: %s" % [dict(u1), dict(u2)])
    assert(u1.name == 'Bob')
    assert(u2['name'] == 'Jill')
    assert(isinstance(u1.last_seen, int))


When we exit the context, all the structs are saved to *Redis* in one pipeline operation.
We can access the fields of the user objects we created as properties or treat the objects like dictionaries.

Let's read those two users we created and modify them.

.. code:: python

    with redpipe.pipeline(autocommit=True) as pipe:
        users = [User('1', pipe=pipe), User('2', pipe=pipe)]
        users[0].change(name='Bobby', last_seen=int(time()), pipe=pipe)
        users[1].remove(['last_seen'])

    print("second batch: %s" % [dict(u1), dict(u2)])

When you pass just the key into the object it reads from the database.
Then we can change the fields we want at any point.
Or we can remove fields we no longer want.

Fields that are undefined can still be accessed as basic strings.


Because the struct is based on a `redpipe.Hash` object, you can access the underlying Hash if you need to extend the functionality of your struct.
From our earlier `User` struct example:

.. code:: python

    username = User.core().hget('1', 'name')

More on this later.



Saving
------


Loading
-------


Deleting
--------


Accessing Properties
--------------------


Extra Fields
------------




