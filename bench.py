#!/usr/bin/env python
"""
Compares redis, pipeline, and redpipe calls.

Over localhost socket.

Network efficiency is already as good as it will get.
Try this with your redis machine.

So far what I am seeing is that redpipe doesn't seem to add any
extra overhead over using redis pipelines.
But it is much easier to work with.

This doesn't compare actual impact of using futures and variable manipulation.
But I am going to bet that for most environments, the impact of network will
far far outweigh any other factor.

Usage
-----

run with `py.test`:

.. code-block:: bash

    py.test ./bench.py

Experimenting with pytest.benchmark plugin:

http://pytest-benchmark.readthedocs.io/


"""
import redislite
import redpipe

# setup. configure here.
# need to make these cli args.
KEY_COUNT = 100
CHUNK_SIZE = 10
redis_client = redislite.StrictRedis()
redpipe.connect_redis(redis_client)


def values_iterator():
    for i in range(0, KEY_COUNT):
        j = i * CHUNK_SIZE
        values = [("%s" % v).encode('utf-8') for v in range(j, j + CHUNK_SIZE)]
        yield values


def bench(r, values):
    results = []
    for i in values:
        key = 'key%s' % i
        r.set(key, i)
        results.append(r.get(key))
    return results


def redispy_bench():
    for values in values_iterator():
        results = bench(redis_client, values)
        assert(results == values)


def redispipeline_bench():
    for values in values_iterator():
        with redis_client.pipeline() as pipe:
            bench(pipe, values)
            results = [v for i, v in enumerate(pipe.execute()) if i % 2 == 1]

        assert (results == values)


def redpipe_bench():
    for values in values_iterator():
        with redpipe.autoexec() as r:
            results = bench(r, values)
        assert (results == values)


def test_redispy(benchmark):
    redis_client.flushall()
    benchmark(redispy_bench)


def test_pipeline(benchmark):
    redis_client.flushall()
    benchmark(redispipeline_bench)


def test_redpipe(benchmark):
    redis_client.flushall()
    benchmark(redpipe_bench)
