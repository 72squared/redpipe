Structs
=======

A *Struct* in redpipe is a dictionary-like object with persistence built in.

Why not an ORM?
---------------
*RedPipe* does not provide you with an ORM solution.
Choose one of the many other packages for python to turn redis into a full-fledged ORM solution.
Choose *Redpipe* if you really care about optimizing your network i/o.

To optimize redis i/o, batch command operations together as much as possible.
ORMs often hide things like automatic unique constraints and indexes beneath the covers.
That makes it tricky to ensure you are batching those operations efficently as possible over the network.

RedPipe exposes these primitives.
Keep the data structures simple.
Inputs and outputs.
This allows us to construct with building blocks that can be pipelined efficiently.

However, we could make things a little bit nicer.

Easy, Efficient I/O
-------------------
You want to be able to load data and persist it into a hash and still preserve the data-type of the original structure.
You could use a string key and serialize in json.
This would defeat the power of redis.

We gave `redpipe.Hash` the ability to type-cast variables stored in redis.
But we could make it more convenient to fetch and save data as objects.

That's where `redpipe.Struct` comes in.


Defining a Struct
-----------------

Here's an example of how to define a *Struct*.

.. code:: python

    # assume we already set up our connection
    from time import time

    # set up a struct object.
    class User(redpipe.Struct):
        _keyspace = 'U'
        _key_name = 'user_id'
        _fields = {
            'name': redpipe.TextField,
            'last_seen': redpipe.IntegerField,
            'admin': redpipe.BooleanField,
        }


A lot of this looks very similar to what we did with `redpipe.Hash`.
That's because struct is built on top of the Hash object.
The struct object is all about syntactic sugar to easily access variables and
be able to manipulate them in a more object oriented manner.


Creating New Structs
--------------------

Now we can create a few user objects.
The first argument is always the key.
The other keyword arguments will be assigned as data.

We pass in a pipeline so we can combine the save operation with other network i/o.

.. code:: python

    with redpipe.pipeline(autocommit=True) as pipe:
        # create a few users
        u1 = User('1', name='Bob', last_seen=int(time()), pipe=pipe)
        u2 = User('2', name='Jill', last_seen=int(time()), pipe=pipe)


    print("first batch: %s" % [dict(u1), dict(u2)])
    assert(u1['name'] == 'Bob')
    assert(u2['name'] == 'Jill')
    assert(isinstance(u1['last_seen'], int))
    assert(u1['user_id'] == '1')
    assert(u2['user_id'] == '2')


When we exit the context, all the structs are saved to *Redis* in one pipeline operation.
It also automatically loads any other data.
Since the commands are batched together, you can write the keys then read the hash in one pass.


Accessing the Data
------------------
*RedPipe* exposes the variables from redis as elements like a dictionary:

.. code:: python

    user['name']

Many ORMS set the data as attributes of the object.
*RedPipe* does not.
This makes it easier to differentiate methods of the object from the data.
It also avoids difficulty of data elements that don't obey the pythonic naming conventions of object attributes.

You can have a element name that would otherwise be illegal.

.. code:: python

    # this wouldn't work, syntax error
    # user.full-name
    # but this will!
    user['full-name']

Modifying Structs
-----------------
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

Using the Underlying Hash
-------------------------
Because the struct is based on a `redpipe.Hash` object, you can access the underlying Hash.
This is pretty helpful if you need to extend the functionality of your struct.
From our earlier `User` struct example:

.. code:: python

    username = User.core().hget('1', 'name')

More on this later.

Setting Data Elements
---------------------
We can also set elements of the Struct just like we would a dictionary:

.. code:: python

    user = User('1')

    with user.pipeline():
        user['first_name'] = 'Jack'
        user['admin'] = True
        user['last_seen'] = int(time.time())

    print(dict(user))

You can see we opened up a pipeline object and then set attributes on the struct.
When we exit the with block, the variables are set on the object and sent to redis.
If you read the values you change before exiting the with block, the values would reflect the original values.
Once the data is changed and committed to redis, it is reflected in the local object.
if you need to bundle the changes with other operations, nest the pipeline.


.. code:: python

    user = User('1')

    with redpipe.pipeline(autocommit=True) as pipe:
        with user.pipeline(pipe):
            user['first_name'] = 'Jack'
            del user['admin']
            user['last_seen'] = int(time.time())
        pipe.execute()

    print(dict(user))

Notice in this example I remove a member from the hash by deleting it.

And if you just need to modify one key, just do it. No pipeline explicitly needed:

.. code:: python

    user = User('1')
    user['name'] = 'James'

This will write data to redis as soon as you assign the variable.


Deleting Structs
----------------

to delete all the data in a struct, use the same syntax as you would for a dictionary:

.. code:: python

    user = User('1')
    user.clear()

Of course you can pipeline it:

.. code:: python

    with redpipe.pipeline(autocommit=True) as pipe:
        user = User('1')
        user.clear(pipe)

I want to create an easy way to delete a Struct without having to read it first.
I could access the core but that seems kludgy.
First class support coming.
Stay tuned.


Extra Fields
------------
I touched on it briefly before, but you can store arbitrary data in a struct too.
The data will be simple string key-value pairs, but you can add type-casting at any point easily.




