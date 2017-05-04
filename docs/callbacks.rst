Callbacks
=========

What if we want to be able to combine the results of multiple operations inside a function?
We need some way to wait until the pipeline executes and then combine the results.
Callbacks to the rescue!

Let me show you what I mean.


(This example uses the `autocommit` flag.
If you missed that section, `read about it here <autocommit.html>`_.)

.. code:: python

    def incr_sum(keys, pipe=None):
        future = redpipe.Future()

        with redpipe.pipeline(pipe, autocommit=True) as pipe:
            results = [pipe.incr(key) for key in keys]

            def cb():
                future.set(sum(results))

            pipe.on_execute(cb)

        return future

    # now get the value on 100 keys
    print(incr_sum(["key%d" % i for i in range(0, 100)]))

We didn't pass in a pipeline to the function.
It pipelines internally.
So if we are just calling the function one time, no need to pass in a pipeline.
But if we need to call it multiple times or in a loop, we can pass a pipeline in.

.. code:: python

    with redpipe.pipeline(autocommit=True) as pipe:
        first = incr_sum(["key%d" % i for i in range(0, 100)], pipe=pipe)
        second = incr_sum(["key%d" % i for i in range(100, 200)], pipe=pipe)

    print(first)
    print(second)



The pipeline context knows how to nest these operations.
As each child context completes it passes its commands and callbacks up a level.
The top pipeline context executes the functions and callbacks, creating the final result.

Use Cases
---------
Callbacks can be used for all kinds of purposes.
In fact, the internals of *RedPipe* take advantage of the callback hook for many different purposes.

Here are some examples:

* Formatting the results of a redis command before returning it
* combining multiple results from several pipelined commands into a single response
* attaching data from a pipelined call to other objects in your application

Gotchas
-------
You can put just about anything you want into a callback.
But try to avoid the trap of making subsequent network calls within a callback when building a function.
It limits the reusability of your modular building block.
The problem is that while the first network round-trip can be pipelined, you won't be able to pipeline the second.

Here's an example of what I mean:

.. code-block:: python

    def incr_if_gt(key, threshold, pipe=None):
        with redpipe.pipeline(pipe, autocommit=True) as pipe:
            future = redpipe.Future()
            value = pipe.get(key)

            def cb():
                if value > threshold:
                    with redpipe.pipeline(autocommit=True) as p:
                        future.set(p.incr(key))
                else:
                    future.set(int(value))

            pipe.on_execute(cb)

            return future

While this code example certainly would work, the `p.incr(key)` command inside could not be pipelined with anything.
So your get command could be pipelined with many other calls, but if it needs to increment the key, it will need to do it all alone.

Bad programmer. No cookie.

Nor can you use the pipe object from the context inside of our parent function.
The reason is because when the pipe exits the with block, it resets the list of commands and callbacks.
