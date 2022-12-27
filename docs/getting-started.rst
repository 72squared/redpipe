Getting Started
===============

Installation
------------

To install, use pip:

.. code-block:: bash

    pip install redpipe

or from source:

.. code-block:: bash

    python setup.py install

Get the Source Code
-------------------
**RedPipe** is actively developed on GitHub.

You can either clone the public repository:

.. code-block:: bash

    git clone git://github.com/72squared/redpipe.git

Or, download the tarball:

.. code-block:: bash

    curl -OL https://github.com/72squared/redpipe/tarball/master


Once you have a copy of the source, install it into your site-packages easily:

.. code-block:: bash

    python setup.py install


Connect redis-py to RedPipe
---------------------------
To use redpipe, You need to bind your redis client instance to *RedPipe*.
Use the standard `redis-py <https://redis-py.readthedocs.io/en/latest/#>`_ client.

.. code-block:: python

    client = redis.Redis()
    redpipe.connect_redis(client)

You only need to do this setup once during application bootstrapping.

This example just sets one connection up as a default, since that is the most common case.
But you can connect multiple redis connections to *RedPipe*.

The goal is to reuse your application's existing redis connection.
RedPipe can be used to build your entire persistence layer in your application.
Or you can use *RedPipe* along side your existing code.

More on this later.


Using RedPipe
-------------
Using *RedPipe* is easy.
We can pipeline multiple calls to redis and assign the results to variables.


.. code-block:: python

    with redpipe.pipeline() as pipe:
        foo = pipe.incr('foo')
        bar = pipe.incr('bar)
        pipe.execute()
    print([foo, bar])


*RedPipe* allocates a pipeline object.
Then we increment a few keys on the pipeline object.
The code looks mostly like the code you might write with redis-py pipelines.
The methods you call on the pipeline object are the same.
But, notice that each `incr` call immediately gets a reference object back in return from each call.
That part looks similar to how `redis-py` works without a pipeline.

The variables (in this case `foo` and `bar`) are empty until the pipeline executes.
If you try to do any operations on them beforehand, it will raise an exception.
Once we complete the `execute()` call we can consume the pipeline results.
These variables, `foo` and `bar`, behave just like the underlying result once the pipeline executes.
You can iterate over it, add it, multiply it, etc.


Reusable Functions
------------------
You can write a function that can work as a standalone chunk of logic
and can also be linked to other pipelines.

Here's a quick example of what I mean:

.. code-block:: python

    def get_foo(pipe=None):
         with redpipe.pipeline(pipe=pipe) as pipe:
            pipe.setnx('foo', 'bar')
            foo = pipe.get('foo')
            pipe.execute()
            return foo

It is easy to see how this works as an standalone function. It looks almost
like what you might write if you were just using redis-py.

.. code-block:: python

    print(get_foo())

This will pipeline the following commands to redis:

* SETNX foo bar
* GET foo

But the magic happens when you link this function with other pipeline objects.

.. code-block:: python

    with redpipe.pipeline() as pipe:
        foo = get_foo(pipe)
        bar = pipe.get('bar')
        pipe.execute()

This example will pipeline these three commands together:

* SETNX foo bar
* GET foo
* GET bar

In this example, the `foo` and `bar` variables are both `redpipe.Future` objects.
They are empty until the `pipe.execute()` happens outside of the function.
The `pipe.execute()`  called inside the `get_foo` function in this case is a `NestedPipeline`.
It passes its stack of commands to the parent pipeline.
That's because we passed a pipeline object into the `get_foo` function.
The function passed that into `redpipe.pipeline` and it returned a NestedPipeline to wrap the one passed in.
