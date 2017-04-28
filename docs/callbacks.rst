Callbacks
=========

What if we want to be able to combine the results of multiple operations inside a function?
We need some way to wait until the pipeline executes and then combine the results.
Callbacks to the rescue!

Let me show you what I mean:

.. code:: python

    def increment_keys(keys, pipe=None):
        ref = redpipe.Future()
        with redpipe.pipeline(pipe, autocommit=True) as pipe:
            results = [pipe.incr(key) for key in keys]
            def cb():
                ref.set(sum(results))
            pipe.on_execute(cb)
        return ref

    # now get the value on 100 keys
    print(increment_keys(["key%d" % i for i in range(0, 100)]))

We didn't pass in a pipeline to the function.
It pipelines internally.
So if we are just calling the function one time, no need to pass in a pipeline.
But if we need to call it multiple times or in a loop, we can pass a pipeline in.

.. code:: python

    with redpipe.pipeline(autocommit=True) as pipe:
        first = increment_keys(["key%d" % i for i in range(0, 100)], pipe=pipe)
        second = increment_keys(["key%d" % i for i in range(100, 200)], pipe=pipe)

    print(first)
    print(second)



The pipeline context knows how to nest these operations.
As each child context completes it passes its commands and callbacks up a level.
The top pipeline context executes the functions and callbacks, creating the final result.

