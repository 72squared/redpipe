Release Notes
=============

Releases prior to **1.0.0** are considered beta.
The api is not officially supported.
We make no guarantees about backward compatibility.

Once the api reaches **1.0.0**, all major and minor release notes will be well documented.
Upgrade notes and any breaking changes will be described here and how to handle them.


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


Earlier Releases
----------------
Releases less than **0.1.0** in this project are considered early alpha and don't deserve special mention.
