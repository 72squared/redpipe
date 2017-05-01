Project Status
==============

RedPipe is based on what I've learned over the past 3 years.
We run a really big open-source redis cluster where all of our data is stored in Redis.
So these ideas were tested by fire in real production environments.

However, RedPipe is a complete rewrite of the original concepts.
I took the opportunity to write it from scratch, taking advantage of all I learned.
There are likely a few bugs that have crept in during this big rewrite and refactor.

That's not an excuse for sloppy code or mistakes.

I believe in well tested code.
If you find issues, `let me know <https://github.com/72squared/redpipe/issues>`_ right away.
I'll fix it and write a regression test.


Road Map
--------
*RedPipe* is close to feature complete.
Here's what I still have on my list:

* improve struct
* distributed hash
* better source-code documentation
* tutorials and examples
* simplify autocommit
* standardize transactions
* pubsub
* geo keyspaces

How Long until a Stable Release?
--------------------------------
Target for a stable release is late May, 2017.
