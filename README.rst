RedPipe
=======
*Pain-Free redis pipelining in python.*

|BuildStatus| |CoverageStatus| |Version| |Python| |Docs|

**RedPipe** is a wrapper around the pipeline component of `redis-py <https://redis-py.readthedocs.io>`_ or `redis-py-cluster <https://redis-py-cluster.readthedocs.io>`_.
It makes it easy to reduce network round trips when talking to *Redis*.

For more general information about redis pipelining, see the `official redis documentation <https://redis.io/topics/pipelining>`_.

Use **RedPipe** to build pipelined redis calls in a modular reusable way.
Rewrite your existing application calls via *redis-py* into efficient batches with only minimal changes.


Requirements
------------

The *redpipe* module requires Python 2.7 or higher.


Installation
------------

To install, use pip:

.. code-block::

    $ pip install redpipe

or from source:

.. code-block::

    $ python setup.py install

Quick Start
-----------
To use redpipe, You need to bind your redis client instance to **RedPipe**.
Use the standard *redis-py* client.

.. code-block:: python

    client = redis.Redis()
    redpipe.connect_redis(client)

You only need to do this setup once during application bootstrapping.

Using **RedPipe** is easy.
We can pipeline multiple calls to redis and assign the results to variables.
This makes pipeline code look and feel similar to the regular interface of *redis-py*.

.. code-block:: python

    with redpipe.pipeline() as pipe:
        foo = pipe.incr('foo')
        bar = pipe.incr('bar)
        pipe.execute()
    print([foo, bar])


Documentation
-------------
Find **RedPipe** documentation on `Read the Docs <http://redpipe.readthedocs.io/en/latest/>`_.


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
