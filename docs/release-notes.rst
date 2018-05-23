Release Notes
=============

2.0.2 (May 23, 2018)
----------------------
rename `_memberparse` to `memberparse` on Hash class to be consistent
with naming conventions of keyparse and valueparse variables.
Also allows access of this variable outside the class which is fine.


2.0.1 (March 15, 2018)
----------------------
ttl on struct objects.
documentation fixes.


2.0.0 (March 8, 2018)
---------------------
to avoid confusion, make the eval command in keyspaces match the
interface defined in redis-py.


1.0.4 (September 28, 2017)
--------------------------
adding support for zcount (somehow missed it before).


1.0.3 (August 10, 2017)
-----------------------
fixing a bug in named connections in structs and keyspaces. fixes issue #2


1.0.2 (June 23, 2017)
---------------------
enable pfcount when using rediscluster.


1.0.1 (May 29, 2017
-------------------
adding an nx option to struct to allow changes to be set only if the properties
don't already exist.


1.0.0 (May 11, 2017
-------------------
No substantive changes from 1.0.0rc3.
Updating notes and removing beta flags.


1.0.0rc3 (May 10, 2017)
-----------------------
Use threads by default when talking to multiple backends in the same pipeline.
You can disable this behavior with `redpipe.disable_threads()`.


1.0.0rc2 (May 9, 2017)
----------------------
Minor changes.

* make the keyspace object call conform to redis-py
* use twine to publish to pypi
* publish wheels


1.0.0rc1 (May 7, 2017)
----------------------
This marks the first RC.
There are a few breaking changes, mostly easily fixed.

* better handling of Nones returned from hmget in Struct
* testing with toxiproxy to simulate slower networks in benchmarks
* using pytest-benchmark tool for benchmark comparisons
* simplifying connections so we can pass in redis or rediscluster
* fixing some compat issues with redis-py interface


0.5.0 (May 5, 2017)
-------------------
More breaking changes to Struct.
Solidifying the api.
Making important simplifications.
This will make it easier to explain and document.

* Struct and Keyspace: simplifying some variable names
* Struct: support a no_op flag to prevent read/write from redis
* Struct: no kwargs as properties of struct. a dict models it better
* Struct: specify fields to load when instantiating
* Struct: reuse remove logic in the update function for elements set to None
* Simplifying task wait and promise to use the TaskManager directly
* Future: better isinstance and is comparison checks
* make it easier to build docs
* adding Docker support for testing many versions of python


0.4.0 (May 4, 2017)
-------------------
* by default, don't use transactions
* autocommit flag renamed to autoexec. *Breaking change*.
* support pickling Struct
* make repr(Struct) more standard
* cleaner connection and pipeline interfaces
* verify redis cluster support with a single-node redis cluster via redislite

0.3.2 (May 3, 2017)
-------------------
After experimenting with some things, simplifying Struct back down.
Some of the methods in Struct will break.
Easier to explain with fewer methods and can still do everything I need to.

* cleaner support for items and iteritems in struct
* support for delete in struct
* fixed a bug with deleting multiple keys in Keyspace objects.
* simplification on json serialization detection
* test flake8 on travis
* test with hiredis

This release also improves the documentation on Struct.
I hadn't bothered much up until this point.
The interface was still solidifying.
Starting to get to a stable place there.

0.3.1 (May 2, 2017)
-------------------
Breaking changes in this release as well.
Can only access data from a struct object like you would a dictionary.
This is an important step because it disambiguates commands from data.
And it enforces one consistent way to access data.
All the methods on the `Struct` give it a dictionary interface.
Easier to explain the mental model this way.

* Improvements to `redpipe.Struct`.
* Documentation improvements.


0.3.0 (April 30, 2017)
----------------------
BIG REFACTOR.
key no longer part of the constructor of Keyspace objects.
Instead, you pass the key name to the method.
This keeps the api identical in arguments in redis-py.
It also allows me to support multi-key operations.
This is a breaking change.

* no need for a compat layer, using six
* standardize key, value, member encoding & decoding by reusing Field interface
* key no longer part of the constructor of Keyspace objects


0.2.5 (April 30, 2017)
----------------------
* support for binary field
* improving encoding and decoding in Keyspaces
* alias iteritems to items on struct
* make fields use duck-typing to validate instead of using isinstance


0.2.4 (April 28, 2017)
----------------------
* better interface for async enable/disable.
* add ability to talk to multiple redis servers in parallel via threads


0.2.3 (April 27, 2017)
----------------------
* renaming datatypes to keyspaces. easier to explain.
* moving documentation from readme into docs/ for readthedocs.
* support for ascii field


0.2.2 (April 26, 2017)
----------------------
* better support and testing of redis cluster
* support for hyperloglog data type
* adding support for more complex field types
* support sortedset lex commands
* support for scanning


0.2.1 (April 24, 2017)
----------------------
* bug fix: make sure accessing result before ready results in a consistent exception type.
* bug fix: issue when exiting with statement from python cli


0.2.0 (April 24, 2017)
----------------------
* make the deferred object imitate the underlying result


0.1.1 (April 23, 2017)
----------------------
* make it possible to typecast fields in the Hash data type
* better support for utf-8
* make result object traceback cleaner

0.1.0 (April 21, 2017)
----------------------

* better pipelining and task management
* better support for multi pipeline use case


Old Releases
------------
Releases prior to **1.0.0** are considered beta.
The api is not officially supported.
We make no guarantees about backward compatibility.

Releases less than **0.1.0** in this project are considered early alpha and don't deserve special mention.
