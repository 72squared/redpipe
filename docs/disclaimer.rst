Disclaimers
===========
*RedPipe* is based on concepts I have been using in a well-tested production environment for a long time.
But some implementation details are fairly new.

There will be bugs.

Please report all issues `here <https://github.com/72squared/redpipe/issues>`_.

I will respond to issues promptly.
Make sure to provide clear explanations of what you are seeing and give steps to reproduce the bug.


Thread Safety
-------------
Thread safety is a stated goal of *RedPipe*.

Redis-Py is considered thread safe by using atomic operations against the GIL when accessing the connection pool.

You should not share objects produced by `redpipe.pipeline()` between threads.
The main issue you will run into is how it enters and exits the with block, resetting the command stack.
Another issue is ordering of commands.
Frankly, I just haven't tested this behavior and don't feel it is important to support it.


You can safely use a different `redpipe.pipeline()` in each thread after setting up your connection.
This is because when the `redpipe.pipeline()` object executes, it obtains a new `redis.pipeline()` object to pass the commands into.
That redis-py pipeline object queues all the commands and then obtains a connection from its pool in a thread-safe way.
Then it packs the commands and sends it over the wire and waits for the response before releasing it back into the connection pool.

If you see any symptoms of unsafe thread behavior, please report it `here <https://github.com/72squared/redpipe/issues>`_.


Character Encoding
------------------
To be honest, I never spent a whole lot of time thinking about character encoding in redis until recently.
Most of the values I manipulate in `redis` are numbers and simple ascii keys.
When I wrote this library originally for python 2, it doesn't make you think about character
encoding vs bytes much at all.
However, I think a good library should fully support proper character encoding.
And since RedPipe is fully tested on python 3, I am making more of an effort to understand the nuances.

If you find a bug, Please report it.


Lua Scripting
-------------
Lua scripting is only barely supported in redis cluster.
You can make it work if you don't bother with script registration or evalsha.
That's because it is too hard to know for sure when running a command whether or not the node in the cluster will have it already.
And it gets especially complicated in pipeline scenarios and pipelined failover scenarios.

So, we choose to always send the full lua script every time.
If you use short lua scripts like I do, it's not a big deal.
the network penalty of bytes over the wire is small compared with the penalty of multiple network round-trips.
And on the redis server, it keeps an internal hash of the compiled lua script.
So there's no additional compilation penalty with sending the Lua script every time.

I know it's not quite as nice this way.
But at least it is functional.
If you have any ideas on how to make this better, let me know.


