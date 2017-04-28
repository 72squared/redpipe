.. RedPipe documentation master file, created by
   sphinx-quickstart on Wed Apr 19 13:22:45 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to RedPipe's documentation!
===================================
*Making Redis pipelines easier to use in python.*

|BuildStatus| |CoverageStatus| |Version| |Python| |Docs|

This project is beta software.
The interface may change.

The code is well tested and rapidly stabilizing.
Check back soon.

Requirements
------------

The *redpipe* module requires Python 2.7 or higher.

What is RedPipe?
----------------
*RedPipe* is a python package designed to help reduce network round trips when talking to redis.
It is a wrapper around *redis-py* or *redis-py-cluster*.

For more general information about redis pipelining, see the `official redis documentation <https://redis.io/topics/pipelining>`_.

Use *redpipe* to build pipelined redis calls in a modular reusable way.
Rewrite your existing application calls via redis-py into efficient batches with only minimal changes.


How Does it Work?
-----------------
*RedPipe* allows you to nest pipelines, attach callbacks, and get references to data before the pipeline executes.
All of these things together allow you to be able to build modular functions that can be combined with other pipelined functions.

Pass a pipeline into multiple functions, collect the results from each function, and then execute the pipeline to hydrate those result objects with data.

What Else Can it Do?
--------------------
You can use just the core of the *redpipe* module but there's a lot of other cool things included.
Be sure to check out the wrappers around keyspaced data-types. And the Struct objects are cool too.


--------


..  toctree::
    :maxdepth: 2
    :caption: Table of Contents:

    getting_started
    rationale
    autocommit
    callbacks
    named_connections
    nested_pipelines
    transactions
    keyspaces
    structs
    latency
    tutorial
    project_status
    redis_cluster
    unicode
    orm
    inspiration
    benchmarking
    testing
    disclaimer
    contributing
    release_notes
    license
    authors
    faq
    todo





Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`


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

