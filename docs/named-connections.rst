Named Connections
=================

So far the examples I've shown have assumed only one connection to `Redis`.
But what if you need to talk to multiple backends?

How to Configure multiple Connections
-------------------------------------
*RedPipe* allows you to set up different connections and then refer to them:

.. code:: python

    redpipe.connect_redis(redis.Redis(port=6379), name='users')
    redpipe.connect_redis(redis.Redis(port=6380), name='messages')


Now I can refer to those named connections inside my functions and throughout my application.

.. code:: python

    with redpipe.pipeline(name='users', autoexec=True) as users:
        users.hset('u{1}', 'name', 'joe')

    with redpipe.pipeline(name='messages', autoexec=True) as messages:
        messages.hset('m{1}', 'body', 'hi there')

If you don't specify a name, it assumes a default connection set up like this:

.. code:: python

    redpipe.connect_redis(redis.Redis(port=6379))

You can actually map the same redis connection to multiple names if you want.
This is good for aliasing names when preparing to split up data, or for testing.

Why Named Connections are Needed
--------------------------------
*RedPipe* allows you to pass in a pipeline to a function, or optionally pass in nothing.
The function doesn't have to think about it.
Just pass the pipe (or None) into `redpipe.pipeline` and everything looks the same under the covers.
But if you have multiple connections, the named pipe passed into the function may not be the same connection.
In this case, we need to always specify what connection we want to use.

If the connection is different than the one passed into the function, redpipe will still batch the two calls together in pipe execute from a logical perspective.
But it needs to send commands to different instances of redis server.
By specifying the connection you want to use with a named connection, you can make sure your command gets sent to the right server.

Talking to Multiple Servers in Parallel
---------------------------------------
When it's time to send those commands to the servers, redpipe batches all commands for each server and sends them out.
*RedPipe* supports asynchronous execution of commands to multiple redis servers via threads.
You can disable this so that the batches sent to each redis server are performed in serial order.

If you talk to only one redis backend connection at a time, *RedPipe* doesn't have to worry about parallel execution.
If you execute a pipeline that combines commands to multiple backends, redpipe will use threads to talk to all backends in parallel.

If you are uncomfortable using threads in your application, you can turn it off at any time via:

.. code-block:: python

    redpipe.disable_threads()

To re-enable this behavior, do:

.. code-block:: python

    redpipe.enable_threads()

If you see any problems with asynchronous execution, `let me know <https://github.com/72squared/redpipe/issues>`_.

