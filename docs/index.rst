.. RedPipe documentation master file, created by
   sphinx-quickstart on Wed Apr 19 13:22:45 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

RedPipe: Pain-Free Pipelining
=============================
Did you try to use Redis pipelines?
Did you get a pounding headache?
Did you throw your laptop in frustration?
Never fear.
**RedPipe** will make you feel better almost immediately.
If you have no idea what Redis is or why you should pipeline commands, `look it up`_ already.


|BuildStatus| |CoverageStatus| |Version| |Python|

Requirements
------------

The *redpipe* module requires Python 3 or higher.

It also requires `redis-py`_.


What is RedPipe?
----------------
**RedPipe** is a wrapper around the pipeline component of `redis-py`_.
It makes it easy to reduce network round trips when talking to *Redis*.
The interface is very similar to `redis-py`.
Pipelined commands work almost like non-pipelined commands.


For more general information about redis pipelining, see the `official redis documentation`_.

Use **RedPipe** to build pipelined redis calls in a modular reusable way.
Rewrite your existing application calls via *redis-py* into efficient batches with only minimal changes.


How Does it Work?
-----------------
RedPipe makes pipeline commands work almost like non-pipelined commands in *redis-py*.
You may have used pipelines before in a few spots as a last-minute performance optimization.
**Redpipe** operates with a different paradigm.
It assumes every call will be pipelined.
And it gives you the tools to do it.

Some concepts sound complicated:

* `futures for data prior to pipeline execution <futures.html>`_
* `callbacks on pipeline execution <callbacks.html>`_
* `nested pipelines <nested-pipelines.html>`_


This documentation will explain all of these concepts and why they are important.
All of these things together allow you to build modular functions that can be combined with other pipelined functions.
You will be able to pass a pipeline into multiple functions, collect the results from each function, and then execute the pipeline to hydrate those result objects with data.

What do I Need to Know to Start?
--------------------------------
If you've used *redis-py*, you know most of what you need already to start using **RedPipe**.

If not, head over there and play with `redis-py`_ first.
Or check out this very easy tutorial on *redis-py* basics:

http://agiliq.com/blog/2015/03/getting-started-with-redis-py/

You'll find the redpipe api looks nearly identical.
That's because **RedPipe** is a wrapper around *redis-py*.

RedPipe adds the ability to pipeline commands in a more natural way.

What Else Can it do?
--------------------

You can use just the core of the **RedPipe**.
Wrap your existing *redis-py* commands with **RedPipe** and profit.
But the library unlocks a few other cool things too:

* `Keyspaces`_ allow you to work more easily with collections of Redis keys.
* `Structs`_ give you an object-oriented interface to working with Redis hashes.

Both components will make it easier to manipulate data in your application.
And they are optimized for maximum network efficiency.
You can pipeline *Keyspaces* and *Structs* just like you can with the core of **RedPipe**.


-------------------------------------


User Documentation
==================
This part of the documentation explains why you need RedPipe.
Then it focuses on step-by-step instructions for getting the most out of RedPipe.

..  toctree::
    :maxdepth: 2

    rationale
    getting-started
    futures
    autoexec
    callbacks
    named-connections
    nested-pipelines
    transactions
    keyspaces
    structs
    latency
    project-status
    redis-cluster
    errors
    unicode
    license
    faq

Community Documentation
=======================
This part of the documentation explains the RedPipe ecosystem.

..  toctree::
    :maxdepth: 2

    testing
    benchmarking
    inspiration
    disclaimer
    contributing
    release-notes
    authors


API Documentation
=================
This part of the documentation provides detailed API documentation.
Dig into the source code and see how everything ties together.
This is what is great about open-source projects.
You can see everything.


..  toctree::
    :maxdepth: 2

    redpipe


.. |BuildStatus| image:: https://travis-ci.org/72squared/redpipe.svg?branch=master
    :target: https://travis-ci.org/72squared/redpipe

.. |CoverageStatus| image:: https://coveralls.io/repos/github/72squared/redpipe/badge.svg?branch=master
    :target: https://coveralls.io/github/72squared/redpipe?branch=master

.. |Version| image:: https://badge.fury.io/py/redpipe.svg
    :target: https://badge.fury.io/py/redpipe

.. |Python| image:: https://img.shields.io/badge/python-3.8,3.9,pypy3-blue.svg
    :target:  https://pypi.python.org/pypi/redpipe/

.. _report any issues: https://github.com/72squared/redpipe/issues

.. _look it up: https://redis.io/topics/pipelining

.. _official redis documentation: https://redis.io/topics/pipelining

.. _redis-py: https://redis-py.readthedocs.io

.. _Keyspaces: keyspaces.html

.. _Structs: structs.html
