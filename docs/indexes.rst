Indexes
=======

Indexes in Redpipe are really just hashes that allow you to shard the index
across multiple keys transparently.

This is not really a core part of redpipe, but the pattern came up so
frequently that it made sense to include it here. And it is a relatively
small peice of code.

For now, refer to the test-case on how to use this.

More detailed documentation will be given in the future as this logic
solidifies.

Here is what a definition might look like:

.. code-block:: python

    class MyIndex(redpipe.Index):
        keyspace = 'my_index'
        connection = 'default'
        shard_count = 1000


This will namespace the shards under the keyspace `my_index:%s:u`.
The string interpolated inside of this keyspace will be a string of digits
between 0 and 999.

To invoke it, you can do:

.. code-block:: python
    with redpipe.pipeline(autoexec=True) as pipe:
        MyIndex.set('foo123', 'test', pipe=pipe)
        res = MyIndex.get('foo123', pipe=pipe)

this will result in a returned value of 'test' for key `foo123`.

Theoretically, you could just as easily use a simple string keyspace. But
then you would need a new key for each value you need to store. this approach
groups all the values together efficiently beneath a single keyspace.