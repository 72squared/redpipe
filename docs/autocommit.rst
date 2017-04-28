Auto-Commit
===========

By default, you must call `pipe.execute` for the commands to be sent to redis.
With the `autocommit` flag, you can save a step:


.. code-block:: python

    with redpipe.pipeline(pipe=pipe, autocommit=True) as pipe:
        foo_count = pipe.incr('foo')

     print(foo_count)


Notice we are using the `with` control-flow structure block.
As you leave the block, it triggers the `__exit__` method on the pipe object.
If the autocommit flag was set, the method verifies no exception was thrown and executes the pipeline.
Otherwise, you must call `pipe.execute()` explicitly.
