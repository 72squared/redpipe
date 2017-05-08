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

Also experimenting with toxiproxy to simulate latency:

https://github.com/Shopify/toxiproxy

toxiproxy-cli create redis -l localhost:26379 -u localhost:6379
toxiproxy-cli toxic add redis -t latency -a latency=2
toxiproxy-cli list
toxiproxy-cli toxic remove redis -n latency_downstream
toxiproxy-cli delete redis


Then you can call this with py.test ./bench.py --port 26379

"""
import redis
import redislite
import redpipe

# setup. configure here.
# need to make these cli args.
KEY_COUNT = 100
CHUNK_SIZE = 10


def build_redis(port):
    if port is None:
        client = redislite.StrictRedis()
    else:
        client = redis.StrictRedis(port=int(port))

    redpipe.reset()
    redpipe.connect_redis(client)
    return client


def values_iterator():
    for i in range(0, KEY_COUNT):
        j = i * CHUNK_SIZE
        values = [("__test_%s" % v).encode('utf-8') for v in
                  range(j, j + CHUNK_SIZE)]
        yield values


def bench(r, values):
    results = []
    for i in values:
        key = 'key%s' % i
        r.set(key, i)
        results.append(r.get(key))
    return results


def redispy_bench(redis_client):
    for values in values_iterator():
        results = bench(redis_client, values)
        assert (results == values)


def redispipeline_bench(redis_client):
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


def test_redispy(port, benchmark):
    redis_client = build_redis(port)
    benchmark(redispy_bench, redis_client=redis_client)


def test_pipeline(port, benchmark):
    redis_client = build_redis(port)
    benchmark(redispipeline_bench, redis_client=redis_client)


def test_redpipe(port, benchmark):
    build_redis(port)
    benchmark(redpipe_bench)
