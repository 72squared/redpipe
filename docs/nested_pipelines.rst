Nested Pipelines
================

The ability to pass one pipeline into another dramatically simplifies your application code.
You can build a function that can perform a complete operation on its own.
You can also effortlessly connect that function to other pipelines.
You've seen it in action elsewhere in the documentation.
Let's dive into what is actually happening under the covers.

How it works
------------
The `redpipe.pipeline` function checks to see if you are passing in another pipeline object or not.
If you pass in nothing, it gives you back a root-level `redpipe.pipelines.Pipeline` object.
I deliberately did not expose this class at the root level of the package.
You never need to instantiate it directly.

This `Pipeline` object will collect your commands.
When `Pipeline.execute` is called, it obtains a `redis.StrictPipeline` and runs your pipelined commands.
Simple.

If you pass in a `Pipeline` object into the `redpipe.pipeline` function, it returns a `redpipe.pipelines.NestedPipeline` object.
Again, you should never need to instantiate it directly.
And you can use `NestedPipeline` exactly like the `Pipeline` object.

When you execute `NestedPipeline`, it passes all the commands and callbacks queued up to its parent.
The parent object is the one you passed into `redpipe.pipeline`.
This may be a `Pipeline` object, or it may be another `NestedPipeline` object.
It cleans itself up and defers execution responsibility to its parent.

The parent now waits for its execution method to be called.
When it does, it keeps passing commands up the chain until it winds up in a `Pipeline` object.
Then the commands get sent off to redis in one big batch.
Then the callbacks are triggered, and everything is ready to use.

How to use it
-------------
This architecture means when you build a function, you don't need to think about what kind of pipeline you are recieving.
It could be a `NestedPipeline` or a `Pipeline` or nothing at all.

Just wrap it all up in `redpipe.pipeline` and do your work.

.. code-block:: python

    class Beer(redpipe.Hash):
        _keyspace = 'B'
        _fields = {
            'beer_name': redpipe.StringField,
            'consumed' redpipe.Integer,
        }

    def get_beer_from_fridge(beer_id, quantity=1, pipe=None):
        with redpipe.pipeline(pipe, autocommit=True) as pipe:
            b = Beer(pipe)
            b.hincrby(beer_id, 'consumed', quantity)
            return b.hgetall(beer_id)


Now I can grab one beer from the fridge at a time.
Or I can take one in each hand.
Or I can grab a case!
And I can do it all in a single network transaction.

.. code-block:: python

    drinks = []
    with redpipe.pipeline(autocommit=True) as pipe:
        drinks.append(get_beer_from_fridge('schlitz', pipe=pipe))
        drinks.append(get_beer_from_fridge('guinness', 6, pipe=pipe))
    print(drinks)

