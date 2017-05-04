.. RedPipe documentation master file, created by
   sphinx-quickstart on Wed Apr 19 13:22:45 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

RedPipe: Pain-Free Pipelining
=============================
Did you try to use Redis pipelines?
Did you give up after an hour of trying?
Did you get a pounding headache?
Did you throw your laptop in frustration?

Never fear.
**RedPipe** will make you feel better almost immediately.

If you have no idea what Redis is or why you should pipeline commands, `look it up <https://redis.io/topics/pipelining>`_ already.


|BuildStatus| |CoverageStatus| |Version| |Python| |Docs|

This project is beta software.
The interface may change.

The code is well tested and rapidly stabilizing.
Check back soon.

Requirements
------------

The *redpipe* module requires Python 2.7 or higher.

It also requires `redis-py <https://redis-py.readthedocs.io>`_ or `redis-py-cluster <https://redis-py-cluster.readthedocs.io>`_.


What is RedPipe?
----------------
*RedPipe* is a python package designed to help reduce network round trips when talking to redis.
It is a wrapper around `redis-py <https://redis-py.readthedocs.io>`_ or `redis-py-cluster <https://redis-py-cluster.readthedocs.io>`_.

For more general information about redis pipelining, see the `official redis documentation <https://redis.io/topics/pipelining>`_.

Use *redpipe* to build pipelined redis calls in a modular reusable way.
Rewrite your existing application calls via redis-py into efficient batches with only minimal changes.


How Does it Work?
-----------------
RedPipe gives you the ability to pipeline commands in a more natural way than the interface provided via `redis-py`.

Some concepts sound complicated:

* `nested pipelines <nested-pipelines.html>`_
* `callbacks on pipeline execution <callbacks.html>`_
* `futures for data prior to pipeline execution <futures.html>`_


This documentation will explain all of these concepts and why they are important.

All of these things together allow you to build modular functions that can be combined with other pipelined functions.

You will be able to pass a pipeline into multiple functions, collect the results from each function, and then execute the pipeline to hydrate those result objects with data.

What do I Need to Know?
-----------------------
If you've used redis-py, you know most of what you need already to start using RedPipe.

If not, head over there and play with `redis-py <https://redis-py.readthedocs.io>`_ first.
Or check out this very easy tutorial on redis-py basics:

http://agiliq.com/blog/2015/03/getting-started-with-redis-py/

You'll find the redpipe api looks nearly identical.
That's because RedPipe is a wrapper around redis-py.

RedPipe adds the ability to pipeline commands in a more natural way.

What Else Can it do?
--------------------
You can use just the core of the *redpipe* module.
That's the wrapper part.


But there's a lot of other cool things included.
Be sure to check out the keyspaced data-types.
And the Struct objects are cool too.

More on this later.


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
    :maxdepth: 3

    redpipe


.. |BuildStatus| image:: https://travis-ci.org/72squared/redpipe.svg?branch=master
    :target: https://travis-ci.org/72squared/redpipe

.. |CoverageStatus| image:: https://coveralls.io/repos/github/72squared/redpipe/badge.svg?branch=master
    :target: https://coveralls.io/github/72squared/redpipe?branch=master

.. |Version| image:: https://badge.fury.io/py/redpipe.svg
    :target: https://badge.fury.io/py/redpipe

.. |Python| image:: https://img.shields.io/badge/python-2.7,3.4,pypy-blue.svg
    :target:  https://pypi.python.org/pypi/redpipe/

.. |Docs| image:: https://readthedocs.org/projects/redpipe/badge/?version=latest
    :target: http://redpipe.readthedocs.org/en/latest/
    :alt: Documentation Status

