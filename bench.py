#!/usr/bin/env python
"""
Really crude and unscientific benchmarking tool.

Hope to make it better soon.

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
"""
import time
import redislite
import redpipe


def bench(r, offset=0, chunk_size=10):
    for i in range(offset, offset + chunk_size):
        key = 'key%s' % i
        r.set('key%s' % i, '1')
        r.get(key)


def redispy_bench(client, iterations=100, chunk_size=10):
    for i in range(0, iterations):
        bench(client, offset=i * chunk_size, chunk_size=chunk_size)
    return iterations * chunk_size


def redispipeline_bench(client, iterations=100, chunk_size=10):
    for i in range(0, iterations):
        with client.pipeline() as pipe:
            bench(pipe, offset=i * chunk_size, chunk_size=chunk_size)
            pipe.execute()
    return iterations * chunk_size


def redpipe_bench(client, iterations=100, chunk_size=10):
    redpipe.connect_redis(client)

    for i in range(0, iterations):
        with redpipe.autoexec() as r:
            bench(r, offset=i * chunk_size, chunk_size=chunk_size)
    return iterations * chunk_size


if __name__ == '__main__':
    iterations = 2000
    chunk_size = 20
    redis_client = redislite.StrictRedis()
    start = time.time()
    redispy_bench(redis_client, iterations=iterations, chunk_size=chunk_size)
    end = time.time()
    elapsed = end - start
    print("redis: %.5f" % elapsed)

    start = time.time()
    redispipeline_bench(redis_client, iterations=iterations,
                        chunk_size=chunk_size)
    end = time.time()
    elapsed = end - start
    print("pipeline: %.5f" % elapsed)

    start = time.time()
    redpipe_bench(redis_client, iterations=iterations, chunk_size=chunk_size)
    end = time.time()
    elapsed = end - start
    print("redpipe: %.5f" % elapsed)
