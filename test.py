#!/usr/bin/env python
import json
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


class FieldsTestCase(unittest.TestCase):

    def test_float(self):
        field = rediswrap.FloatField
        self.assertTrue(field.validate(2.12))
        self.assertTrue(field.validate(0.12456))
        self.assertFalse(field.validate(''))
        self.assertFalse(field.validate('a'))
        self.assertFalse(field.validate('1'))
        self.assertEqual(field.to_persistence(1), '1')
        self.assertEqual(field.to_persistence(1.2), '1.2')
        self.assertEqual(field.to_persistence(1.2345), '1.2345')
        self.assertEqual(field.from_persistence('1'), 1)
        self.assertEqual(field.from_persistence('1.2'), 1.2)
        self.assertEqual(field.from_persistence('1.2345'), 1.2345)
        self.assertRaises(ValueError, lambda: field.from_persistence('x'))

    def test_int(self):
        field = rediswrap.IntegerField
        self.assertTrue(field.validate(2))
        self.assertTrue(field.validate(12456))
        self.assertFalse(field.validate(''))
        self.assertFalse(field.validate('a'))
        self.assertFalse(field.validate('1'))
        self.assertTrue(field.validate(0))
        self.assertFalse(field.validate(0.1))
        self.assertEqual(field.to_persistence(1), '1')
        self.assertEqual(field.from_persistence('1234'), 1234)
        self.assertRaises(ValueError, lambda: field.from_persistence('x'))

    def test_text(self):
        field = rediswrap.TextField
        self.assertFalse(field.validate(1))
        self.assertFalse(field.validate(False))
        self.assertFalse(field.validate(0.12456))
        self.assertTrue(field.validate('dddd'))
        self.assertTrue(field.validate(json.loads('"15\u00f8C"')))
        self.assertTrue(field.validate(''))
        self.assertTrue(field.validate('a'))
        self.assertTrue(field.validate('1'))
        self.assertEqual(field.to_persistence('1'), b'1')
        self.assertEqual(field.to_persistence('1.2'), b'1.2')
        self.assertEqual(field.to_persistence('abc123$!'), b'abc123$!')
        sample = json.loads('"15\u00f8C"')
        self.assertEqual(
            field.from_persistence(field.to_persistence(sample)),
            sample
        )


class ModelTestCase(BaseTestCase):

    class User(rediswrap.Model):
        namespace = 'U'
        _fields = {
            'first_name': rediswrap.TextField,
            'last_name': rediswrap.TextField,
        }

    def test(self):
        u = self.User('1')
        self.assertFalse(u.persisted)
        self.assertEqual(dict(u), {'_key': '1'})
        u = self.User('1', first_name='Fred', last_name='Flintstone')
        self.assertTrue(u.persisted)
        u = self.User('1')
        self.assertTrue(u.persisted)
        self.assertEqual(u.first_name, 'Fred')
        self.assertIn('U', str(u))
        self.assertIn('1', str(u))
        self.assertEqual(u.last_name, 'Flintstone')
        self.assertRaises(AttributeError, lambda: u.non_existent_field)
        u.save(first_name='Wilma')
        self.assertEqual(u.first_name, 'Wilma')
        u = self.User('1')
        self.assertEqual(u.first_name, 'Wilma')
        self.assertEqual('Wilma', u['first_name'])
        u.delete()
        self.assertFalse(u.persisted)

    def create_user(self, k, pipe=None, **kwargs):
        u = self.User(
            "%s" % k,
            pipe=pipe,
            first_name='first%s' % k,
            last_name='last%s' % k,
            email='user%s@test.com' % k,
            **kwargs
        )
        return u

    def test_pipeline(self):
        user_ids = ["%s" % i for i in range(1, 3)]
        with rediswrap.PipelineContext() as pipe:
            users = [self.create_user(i, pipe=pipe, a=None, b='123')
                     for i in user_ids]
            self.assertEqual(
                [u.persisted for u in users],
                [False for _ in user_ids])
            retrieved_users = [self.User(i, pipe=pipe) for i in user_ids]

        # before executing the pipe (exiting the with block),
        # the data will not show as persisted.
        # once pipe execute happens, it is persisted.
        self.assertEqual(
            [u.persisted for u in users],
            [True for _ in user_ids])

        self.assertEqual(
            [u.b for u in retrieved_users],
            ['123' for _ in retrieved_users])

    def test_fields(self):
        class Multi(rediswrap.Model):
            namespace = 'M'
            _fields = {
                'boolean': rediswrap.BooleanField,
                'integer': rediswrap.IntegerField,
                'float': rediswrap.FloatField,
                'text': rediswrap.TextField,
            }

        data = {
            'text': 'xyz',
            'integer': 5,
            'boolean': False,
            'float': 2.123}

        m = Multi('m1', **data)
        expected = {'_key': 'm1'}
        expected.update(data)
        self.assertEqual(dict(m), expected)
        self.assertEqual(m.boolean, data['boolean'])
        self.assertEqual(m['boolean'], data['boolean'])
        self.assertEqual(m.get('boolean'), data['boolean'])
        self.assertEqual(m.get('non_existent', 'foo'), 'foo')
        self.assertEqual(m.get('non_existent'), None)
        expected = {'_key': 'm2'}
        expected.update(data)
        m = Multi('m2', **data)
        self.assertEqual(dict(m), expected)
        m = Multi('m2')
        self.assertEqual(dict(m), expected)

        self.assertRaises(
            rediswrap.InvalidFieldValue,
            lambda: Multi('m3', text=123))

        self.assertRaises(
            rediswrap.InvalidFieldValue,
            lambda: Multi('m3', boolean='abc'))


class ConnectTestCase(unittest.TestCase):

    def test(self):
        r = redislite.StrictRedis()
        rediswrap.connect(r)
        rediswrap.connect(r)
        self.assertRaises(
            rediswrap.AlreadyConnected,
            lambda: rediswrap.connect(redislite.StrictRedis()))
        rediswrap.disconnect()
        rediswrap.connect(redislite.StrictRedis())

        # tear down the connection
        rediswrap.disconnect()

        # calling it multiple times doesn't hurt anything
        rediswrap.disconnect()


if __name__ == '__main__':
    unittest.main(verbosity=2)
