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

* distributed hash, so we can spread an index out over multiple keys
* better benchmarking
* Tutorials and Examples

Another way of defining the roadmap is listing what I expect **NOT** to be supported:

* Unique Constraints on Struct
* one-to-many indexes on Struct
* many-to-many indexes on Struct
* required fields on Struct

All of these start forcing me down the road of requiring network i/o in ways that you can't control.
These operations are best left up to your application logic to handle.

You can still build indexes and unique constraints using redpipe SortedSets, Lists, Sets, Hashes etc.
But you do so separately from Struct as their own first-class objects.

This allows you to access and control the indexes separately from the objects.
Don't see this as a deficiency in the framework.
See it as a feature.


How Long until a Stable Release?
--------------------------------
Target for a stable release is late May, 2017.
This code represents an RC release and should be fully functional.
Try it out in your projects and let me know how it goes.
