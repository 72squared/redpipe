Structs
=======

A *Struct* in redpipe is a dictionary-like object with persistence built in.

Easy, Efficient I/O
-------------------
You want to be able to load data and persist it into a hash and still preserve the data-type of the original structure.
We gave `redpipe.Hash` the ability to type-cast variables stored in redis.
But we could make it more convenient to fetch and save data as objects.

That's where `redpipe.Struct` comes in.


Defining a Struct
-----------------

Here's an example of how to define a *Struct*.

.. code-block:: python

    # assume we already set up our connection

    # set up a struct object.
    class User(redpipe.Struct):
        keyspace = 'U'
        key_name = 'user_id'
        fields = {
            'name': redpipe.TextField,
            'last_seen': redpipe.IntegerField,
            'admin': redpipe.BooleanField,
            'page_views': redpipe.IntegerField,
        }


A lot of this looks very similar to how we defined `redpipe.Hash`.
That's because struct is built on top of the Hash object.
It allows you to access data from a hash in a more object oriented manner.

The `Struct` does not enforce required fields on any of this data.
Just as a redis hash object does not.
It is up to your application logic to enforce these constraints.

The rule is that if the element is in the hash, it will be coerced into the appropriate data type by the `fields` definition.
If an element in the hash is not mentioned in the `fields` it is coerced into a `TextField`.

You can override this default behavior by defining `valueparse`.

.. code-block:: python

    class User(redpipe.Struct):
        keyspace = 'U'
        key_name = 'user_id'
        fields = {
            # ...
        }
        valueparse = redpipe.AsciiField

This example will force all values not listed in `fields` to be set as ascii values in redis.
(It does not coerce values already in redis to be ascii tho.
It will treat them as text.)

You can specify an alternate redis connection if you are using multiple redis connections in your app.

.. code-block:: python

    class User(redpipe.Struct):
        keyspace = 'U'
        key_name = 'user_id'
        fields = {
            # ...
        }
        connection = 'users'

The string value `users` refers to a connection you have added in application bootstrapping.
See the `Named Connections <./named-connections.html>`_ section of this documentation.


Creating New Structs
--------------------

Let's create a few user objects using our `Struct`.
The first argument is always either the key or the data.

We pass in a pipeline so we can combine the save operation with other network i/o.

.. code-block:: python

    with redpipe.autoexec() as pipe:
        # create a few users
        ts = int(time.time())
        u1 = User({'user_id': '1', 'name': 'Jack', 'last_seen': ts}, pipe=pipe)
        u2 = User({'user_id': '2', 'name': 'Jill', 'last_seen': ts}, pipe=pipe)

    # these model objects print out a json dump representation of the data.
    print("first batch: %s" % [u1, u2])

    # we can access the data like we would dictionary keys
    assert(u1['name'] == 'Jack')
    assert(u2['name'] == 'Jill')
    assert(isinstance(u1['last_seen'], int))
    assert(u1['user_id'] == '1')
    assert(u2['user_id'] == '2')


When we exit the context, all the structs are saved to *Redis* in one pipeline operation.
It also automatically loads the other fields in the hash.
Since the commands are batched together, you can write the fields then read the hash in one pass.
If you don't want it to read, you can set the fields to an empty array.

Accessing the Data
------------------
*RedPipe* exposes the variables from redis as elements like a dictionary:

.. code-block:: python

    user = User({'user_id': '1', 'name': 'Jack'})
    assert(user['name'] == 'Jack')


Here, we accessed the name field of the redis hash as a dictionary element on the user object.
This data is loaded from redis inside the object on instantiation by calling `hgetall()` on the key.
The data is cached inside the struct.

You can coerce the objects into dictionaries.


.. code-block:: python

    user = User({'user_id': '1', 'name': 'Jack'})
    assert(dict(user) == user)

This just takes all the internal data and returns it as a dictionary.
If you don't define a `_key_name` on the object, it defaults to the field name '_key'.
This primary key is not stored inside the hash. It is embedded in the redis object key name.
This is more efficient that storing it both in the name of the key and as an element of the hash.
It also avoids problems of accidentally creating a mismatch.


You can compare the user `Struct` to a dictionary for equality.


.. code-block:: python

    user = User({'user_id': '1', 'name': 'Jack'})
    assert(dict(user) == user)

There is an `__eq__` magic method on `Struct` that allows this comparison.


You can iterate on the object like a dictionary:

.. code-block:: python

    user = User({'user_id': '1', 'name': 'Jack'})
    assert({k: v for k, v in user.items()} == user)


You can see the user object has an `items` method.
There is also a `iteritems` method for backward compatibility with python 2.
The `iteritems` method is a generator, whereas `items` returns a list of key/value tuples.


You can access an unknown data element like you would a dictionary:

.. code-block:: python

    user = User({'user_id': '1', 'name': 'Jack'})
    assert(user.get('name', 'unknown') == 'Jack')

The `get` method allows you to pass in a default if no key is found.
It defaults to `None`.

You can check for key existence:

.. code-block:: python

    user = User({'user_id': '1', 'name': 'Jack'})
    assert('name' in user)
    assert('non-existent-name' not in user)


The magic method `__contains__` looks for the key in the internal dictionary, or the `_key_name` field.

You can check the length of a struct:

.. code-block:: python

    user = User({'user_id': '1', 'name': 'Jack'})
    assert(len(user) == 2)


This will include the primary key, so it should never be less than 1.
A `Struct` object will always have a primary key.

You can get the keys of a struct:

.. code-block:: python

    user = User({'user_id': '1', 'name': 'Jack'})
    # returns a list but we don't know the order
    # coerce to a set for comparison
    assert(set(user.keys()) == {'user_id', 'name'})

The `_key_name` will show up in this list.
If no `_key_name` is defined, you will see `_key` in the list of keys.

Many ORMS set the data as attributes of the object.
*RedPipe* does not.
This makes it easier to differentiate methods of the object from the data.
It also avoids difficulty of data elements that don't obey the pythonic naming conventions of object attributes.

You can have a element name that would otherwise be illegal.

.. code-block:: python

    # this wouldn't work, syntax error
    # user.full-name
    # but this will!
    user['full-name']


Modifying Structs
-----------------
Let's read those two users we created and modify them.

.. code-block:: python

    with redpipe.autoexec() as pipe:
        users = [User('1', pipe=pipe), User('2', pipe=pipe)]
        ts = int(time.time())
        users[0].update({'name':'Bobby', 'last_seen': ts}, pipe=pipe)
        users[1].remove(['last_seen'])

    print([dict(u1), dict(u2)])

When you pass just the key into the object it reads from the database.
Then we can change the fields we want at any point.
Or we can remove fields we no longer want.

Fields that are undefined can still be accessed as basic strings.

We can remove a field and return it like we would popping an item from a dict:

.. code-block:: python

    with redpipe.autoexec() as pipe:
        user = User({'user_id': '1', 'name': 'Jack'}, pipe=pipe)
        name = user.pop('name', pipe=pipe)

    assert(name == 'Jack')
    assert(user.get('name', None) is None)


This doesn't just pop the data from the local data structure.
It also pops it from redis.
Use at your own risk.

You don't have to use a pipeline if you don't want to:

.. code-block:: python

    user = User({'user_id': '1', 'name': 'Jack'})
    name = user.pop('name')

    assert(name == 'Jack')
    assert(user.get('name', None) is None)


But then you pay for two network round-trips.

If you want to ensure you don't modify redis accidentally, coerce your user object into a dictionary.

You can increment a field:

.. code-block:: python

    with redpipe.autoexec() as pipe:
        user = User({'user_id': '1', 'name': 'Jack'}, pipe=pipe)
        user.incr('page_views', pipe=pipe)

    assert(user['page_views'], 1)

As with the pop example, you can use a pipe or not.
There's also a `decr` method which is the inverse.


Using the Underlying Hash
-------------------------
Because the struct is based on a `redpipe.Hash` object, you can access the underlying Hash.
This is pretty helpful if you need to extend the functionality of your struct.
From our earlier `User` struct example:

.. code-block:: python

    username = User.core().hget('1', 'name')

More on this later.


Deleting Structs
----------------

to delete all the data in a struct, use the same syntax as you would for a dictionary:

.. code-block:: python

    user = User('1')
    user.clear()


Of course you can pipeline it:

.. code-block:: python

    with redpipe.autoexec() as pipe:
        user = User('1')
        user.clear(pipe)


If you need to delete a record without loading the record, you can call the Struct class method:

.. code-block:: python

    with redpipe.autoexec() as pipe:
        User.delete(['1', '2', '3'], pipe=pipe)




Extra Fields
------------
I touched on it briefly before, but you can store arbitrary data in a struct too.

.. code-block:: python

    user = User({'user_id': '1', 'arbitrary_field': 'foo'})
    assert(user['arbitrary_field'] == 'foo')

The data will be simple string key-value pairs by default.
But you can add type-casting at any point easily in the `fields` dictionary.

Why Struct and not Model?
-------------------------
I chose the name `Struct` because it implies a single, standalone data structure.
You clearly define data structure of the struct.
And you can instantiate the struct with many records.
The word *Struct* doesn't imply indexes or one-to-many relationships the way the word *Model* does.


Why no ORM?
-----------
An Object-Relational Mapping can make life much simpler.
Automatic indexes, foreign keys, unique constraints, etc.
It hides all that pesky complexity from you.
If you want a good ORM for redis, check out `ROM <http://pythonhosted.org/rom/rom.html#documentation>`_.
Or `redish <https://readthedocs.org/projects/redish/>`_.
Both are pretty cool.

*RedPipe* does not provide you with an ORM solution.

Choose *Redpipe* if you really care about optimizing your network i/o.

To optimize redis i/o, you need to batch command operations together as much as possible.
ORMs often hide things like automatic unique constraints and indexes beneath the covers.
It bundles lots of multi-step operations together, where one operation feeds another.
That makes it tricky to ensure you are batching those operations efficently as possible over the network.

RedPipe exposes low level redis command primitives.
Inputs and outputs.
This allows you to construct building blocks that can be pipelined efficiently.
