#!/usr/bin/env python

import unittest
import redislite
import rediswrap
from rediswrap import Client, Pipeline, NestedPipeline


class BaseTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.r = redislite.StrictRedis()
        rediswrap.connect(cls.r)

    @classmethod
    def tearDownClass(cls):
        cls.r = None
        rediswrap.disconnect()

    def setUp(self):
        self.r.flushall()

    def tearDown(self):
        self.r.flushall()


class ClientTestCase(BaseTestCase):

    def test_string(self):
        r = Client(self.r)
        res = r.set('foo', b'bar')
        self.assertEqual(res.result, 1)
        res = r.get('foo')
        self.assertEqual(res.result, b'bar')

    def test_client_pipeline(self):
        r = Client(self.r)
        p = r.pipeline()
        p.set('foo', b'bar')
        g = p.get('foo')

        # can't access it until it's ready
        self.assertRaises(AttributeError, lambda: g.result)
        p.execute()

        self.assertEqual(g.result, b'bar')

    def test_attributes(self):
        self.assertIsInstance(Client(self.r).RESPONSE_CALLBACKS, dict)


class PipelineTestCase(BaseTestCase):

    def test_string(self):
        p = Pipeline(self.r.pipeline())

        p.set('foo', b'bar')
        g = p.get('foo')

        # can't access it until it's ready
        self.assertRaises(AttributeError, lambda: g.result)
        p.execute()

        self.assertEqual(g.result, b'bar')

    def test_zset(self):
        p = Pipeline(self.r.pipeline())

        p.zadd('foo', 1, 'a')
        p.zadd('foo', 2, 'b')
        p.zadd('foo', 3, 'c')
        z = p.zrange('foo', 0, -1)

        # can't access it until it's ready
        self.assertRaises(AttributeError, lambda: z.result)
        p.execute()

        self.assertEqual(z.result, [b'a', b'b', b'c'])

    def test_callback(self):
        p = Pipeline(self.r.pipeline())
        results = {}

        def incr(k, v):
            ref = p.incrby(k, v)

            def cb():
                results[k] = ref.result
            p.on_execute(cb)

        incr('foo', 1)
        incr('bar', 2)
        incr('bazz', 3)
        self.assertEqual(results, {})
        p.execute()
        self.assertEqual(results, {
            'foo': 1,
            'bar': 2,
            'bazz': 3
        })

    def test_attributes(self):
        self.assertIsInstance(
            Pipeline(self.r.pipeline()).RESPONSE_CALLBACKS,
            dict)

    def test_reset(self):
        with Pipeline(self.r.pipeline()) as p:
            ref = p.zadd('foo', 1, 'a')
        self.assertEqual(p._callbacks, [])
        self.assertEqual(p._stack, [])
        self.assertRaises(AttributeError, lambda: ref.result)
        self.assertEqual(self.r.zrange('foo', 0, -1), [])

        with Pipeline(self.r.pipeline()) as p:
            ref = p.zadd('foo', 1, 'a')
            p.execute()
        self.assertEqual(p._callbacks, [])
        self.assertEqual(p._stack, [])
        self.assertEqual(ref.result, 1)
        self.assertEqual(self.r.zrange('foo', 0, -1), [b'a'])

        p = Pipeline(self.r.pipeline())
        ref = p.zadd('foo', 1, 'a')
        p.reset()
        p.execute()
        self.assertRaises(AttributeError, lambda: ref.result)


class NestedPipelineTestCase(BaseTestCase):
    def test(self):
        pipe = Pipeline(self.r.pipeline())
        nested_pipe = NestedPipeline(pipe)
        ref = nested_pipe.zadd('foo', 1, 'a')
        nested_pipe.execute()
        self.assertRaises(AttributeError, lambda: ref.result)
        pipe.execute()
        self.assertEqual(ref.result, 1)

    def test_reset(self):
        parent_pipe = Pipeline(self.r.pipeline())
        data = []
        with NestedPipeline(parent_pipe) as p:
            ref = p.zadd('foo', 2, 'b')

            def cb():
                data.append(ref.result)

            p.on_execute(cb)
            p.execute()
        self.assertEqual(data, [])
        self.assertRaises(AttributeError, lambda: ref.result)
        self.assertEqual(self.r.zrange('foo', 0, -1), [])
        parent_pipe.execute()

        self.assertEqual(p._callbacks, [])
        self.assertEqual(p._stack, [])
        self.assertEqual(ref.result, 1)
        self.assertEqual(data, [1])
        self.assertEqual(self.r.zrange('foo', 0, -1), [b'b'])

    def test_attributes(self):
        self.assertIsInstance(
            NestedPipeline(Pipeline(self.r.pipeline())).RESPONSE_CALLBACKS,
            dict)


class StringCollectionTestCase(BaseTestCase):
    def test(self):
        rediswrap.connect_pipeline(self.r.pipeline)

        class Foo(rediswrap.String):
            namespace = 'F'
        with rediswrap.PipelineContext() as pipe:
            f = Foo('1', pipe=pipe)
            set_ref = f.set('bar')
            get_ref = f.get()

        self.assertEqual(set_ref.result, 1)
        self.assertEqual(get_ref.result, b'bar')

if __name__ == '__main__':
    unittest.main(verbosity=2)
