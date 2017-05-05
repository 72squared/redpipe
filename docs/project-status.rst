Project Status
==============

RedPipe is based on what I've learned over the past 3 years.
We run a really big open-source redis cluster where all of our data is stored in Redis.
So these ideas were tested by fire in real production environments.

However, RedPipe is a complete rewrite of the original concepts.
I took the opportunity to write it from scratch, taking advantage of all I learned.
There may be a few bugs that have crept in during this big rewrite and refactor.

That's not an excuse for sloppy code or mistakes.

I believe in well tested code.
If you find issues, `let me know <https://github.com/72squared/redpipe/issues>`_ right away.
I'll fix it and write a regression test.

Road Map
--------
Here's my current backlog:

* required fields in Struct
* flag to restrict a Struct to only get and set defined fields
* distributed hash, so we can spread an index out over multiple keys
* better support for iterating through keys when on redis-py-cluster

The last one may best be fixed by patching redis-py-cluster to support scan.



How Long until a Stable Release?
--------------------------------
Target for a stable release is late May, 2017.
