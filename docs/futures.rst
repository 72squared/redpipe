Futures
=======
When redis clients communicate with the server, they send a command, then wait for the response.
The `redis-py` client reflects this design choice.
But when you pipeline, you don't wait.
You queue up a bunch of commands.
Then you execute them.
Then you gather these results and feed them back to where they need to go.

This forces the code invoking the execute method to *know* what to do with all the responses.

That's pretty inconvenient.

The **RedPipe** `Future` makes it possible to get a reference to the data before pipeline execute happens.
A `Future` is a contract that says the result of this redis command will be populated into this object once the pipeline executes.
And you can use a `Future` just like you would the actual result.
But only after the pipeline executes.
If you try to use it prior to that, it raises a `redpipe.ResultNotReady` exception.
Kaboom!

Well, what use is that?

For one, it makes it easier to assign it a variable name in the context of calling the command.
No confusion about how to get the result you need from the array of results returned by `pipeline.execute`.
Unlike `redis-py` **RedPipe** does not return an array of results on the `execute` call.
You already have the result as a variable returned from the command you called initially.

More importantly, the `Future` can be passed into a python closure that can do additional work.
Tell the pipeline to execute your closure callback function after it runs.
Now you have a powerful mechanism to format those results and build modular reusable components.

Still don't quite see how? Read more about `callbacks <callbacks.html>`_.

Gotchas
-------

There are a few gotchas to watch out for:

* isinstance() checks
* identity checks like: future is None
* trying to mutate the object like this: future += 1

You can always type cast the object into the type you expect
if you need this behavior.

.. code-block:: python

    f = Future()
    f.set(1)

    # `f is 1` fails
    assert(int(f) is 1) # works

This doesn't work so well for is None checks.
You can't type-cast to None.
You can use equality checks though.

.. code-block:: python

    f = Future()
    f.set(None)
    assert(f == None)

This is frowned upon by most lint-checks who think the `is` comparison is more appropriate.
But if you do an `is` comparison, that compares the object ids.
And there's no way for **RedPipe** to wrap that.

UGH!

In response, I came up a handy IS method.

.. code-block:: python

    f = Future()
    f.set(None)
    assert(f.IS(None))


Or you can access the underlying result:

.. code-block:: python

    f = Future()
    f.set(None)

    assert(f.result is None)

Hope that helps.

Examples
--------
Other than those few caveats, you should be able to access a future object
just like the underlying result.

Here are some examples if your result is numeric.

.. code-block:: python

    future = Future()
    future.set(1)
    assert(future == 1)
    assert(future != 2)
    assert(future > 0)
    assert(future < 2)
    assert(future >= 1)
    assert(future <= 1)
    assert(bool(future))
    assert(float(future) == 1.0)
    assert(future + 1 == 2)
    assert(future * 2 == 2)
    assert(future ^ 1 == 0)
    assert(repr(future) == '1')

And here is an example if your future is a list:

.. code-block:: python

    future = Future()
    future.set([1])
    assert(future == [1])
    assert([v for v in future] == [1])
    assert(future + [2] == [1, 2])

And here is a dictionary:

.. code-block:: python

    future = Future()
    future.set({'a': 1})
    assert(future == {'a': 1})
    assert(dict(future) == {'a': 1})
    assert({k: v for k, v in future.items()} == {'a': 1})

There are many more operations supported but these are the most common.
`Let me know <https://github.com/72squared/redpipe/issues>`_ if you need
more examples or explanation.

Json Serialization
------------------
The default json serializer doesn't know anything about **RedPipe** `Futures`.
When it encounters a `Future`, the json encoder would normally blow up.

I monkey-patched it so it will serialize correctly.

.. code-block:: python

    future = Future()
    future.set({'a': 1})
    json.dumps(future)

The monkey-patching is pretty simple.
Take a look at the `source code`_ if you are interested.
If you have serious objections to this hack, `let me know`_.

If you used a different json serializer, I can't help you.
You may be able to patch those libraries on your own.
Or you could also explicitly extract the result or type-cast before encoding as json.

.. _source code: https://github.com/72squared/redpipe/blob/master/redpipe/futures.py

.. _let me know: https://github.com/72squared/redpipe/issues
