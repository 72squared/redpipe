Unicode Support
===============

If you use `RedPipe.pipeline` objects directly, you are writing raw bytes into redis and reading them out.

Go down a higher level of abstraction in the Keyspaces, and all keys and values are unicode characters stored as utf-8 bytes in redis.
When we read the bytes out of redis we decode them back into strings in python.

Python 3 is much pickier about this. Python 2 doesn't force you to think about it and often does the right thing, but can be error prone.

I'm no expert at unicode or character encoding so if you see a bug let me know and I'll try to fix it.

Still working on more tests in this area.

Why not make all of the data utf-8 compliant?

There are some operations, like redis DUMP and redis RESTORE where the binary data shouldn't be decoded as unicode.
It's a raw binary data representation.
In other cases you may decide to pickle objects and store them in redis.
*RedPipe* should be able to support all of this.


This part of the library is less mature than other aspects of the code.

Use at your own risk.

Please report any `issues <https://github.com/72squared/redpipe/issues>`_.