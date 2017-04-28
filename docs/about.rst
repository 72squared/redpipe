What is RedPipe?
================
The *redpipe* module is a python package designed to help you make best use of network i/o when talking to redis.
It is a wrapper around *redis-py* or *redis-py-cluster*.
Use *redpipe* to build pipelined redis calls in a modular reusable way.

For more general information about redis pipelining, see the `official redis documentation <https://redis.io/topics/pipelining>`_.

How it Works, From 50,000 Feet
------------------------------
Redpipe allows you to nest pipelines, attach callbacks, and get references to data before the pipeline executes.
All of these things together allow you to be able to build modular functions that can be combined with other pipelined functions.

Pass a pipeline into multiple functions, collect the results from each function, and then execute the pipeline to hydrate those result objects with data.

Extra Goodies Included
----------------------
You can use just the bare bones of the *redpipe* module but there's a lot of other cool things included.
Be sure to check out the wrappers around keyspaced data-types. And the Struct objects are cool too.
