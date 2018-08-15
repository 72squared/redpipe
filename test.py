#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import json
import unittest
import uuid
import time
import redis
import redislite
import rediscluster
import rediscluster.exceptions
import redpipe
import redpipe.tasks
import six
import pickle
import socket

# Tegalu: I can eat glass ...
utf8_sample = u'నేను గాజు తినగలను మరియు అలా చేసినా నాకు ఏమి ఇబ్బంది లేదు'


class SingleNodeRedisCluster(object):
    __slots__ = ['node', 'port', 'client']

    def __init__(self, starting_port=7000, strict=True):
        port = starting_port
        while port < 55535:

            try:
                self._check_port(port)
                self._check_port(port + 10000)
                break
            except IOError:
                pass
            port += 1

        self.port = port
        self.node = redislite.StrictRedis(
            serverconfig={
                'cluster-enabled': 'yes',
                'port': port
            }
        )
        self.node.execute_command('CLUSTER ADDSLOTS', *range(0, 16384))
        for i in range(0, 100):
            try:
                self.node.set('__test__', '1')
                self.node.delete('__test__')
                break
            except redis.exceptions.ResponseError:
                pass

            time.sleep(0.1)

        klass = rediscluster.StrictRedisCluster if strict \
            else rediscluster.RedisCluster
        self.client = klass(startup_nodes=[
            {'host': '127.0.0.1', 'port': port}
        ])

    @staticmethod
    def _check_port(port):
        s = socket.socket()
        try:
            s.bind(('', port))
        finally:
            s.close()

    def shutdown(self):
        if self.client:
            self.client.connection_pool.disconnect()
            self.client = None

        if self.node:
            self.node.execute_command('CLIENT KILL SKIPME yes')
            self.node._cleanup()  # noqa

            self.node = None

    def __del__(self):
        self.shutdown()


class BaseTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.r = redislite.StrictRedis()
        redpipe.connect_redis(cls.r)

    @classmethod
    def tearDownClass(cls):
        cls.r = None
        redpipe.reset()

    def setUp(self):
        self.r.flushall()

    def tearDown(self):
        self.r.flushall()


class PipelineTestCase(BaseTestCase):
    def test_string(self):
        p = redpipe.pipeline()

        p.set('foo', b'bar')
        g = p.get('foo')

        # can't access it until it's ready
        self.assertRaises(redpipe.ResultNotReady, lambda: g.result)
        p.execute()

        self.assertEqual(g, b'bar')

    def test_zset(self):
        p = redpipe.pipeline()

        p.zadd('foo', 1, 'a')
        p.zadd('foo', 2, 'b')
        p.zadd('foo', 3, 'c')
        z = p.zrange('foo', 0, -1)

        # can't access it until it's ready
        self.assertRaises(redpipe.ResultNotReady, lambda: z.result)
        p.execute()

        self.assertEqual(z, [b'a', b'b', b'c'])

    def test_callback(self):
        p = redpipe.pipeline()
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

    def test_reset(self):
        with redpipe.pipeline() as p:
            ref = p.zadd('foo', 1, 'a')
        self.assertEqual(p._callbacks, [])
        self.assertEqual(p._stack, [])
        self.assertRaises(redpipe.ResultNotReady, lambda: ref.result)
        self.assertEqual(self.r.zrange('foo', 0, -1), [])

        with redpipe.pipeline() as p:
            ref = p.zadd('foo', 1, 'a')
            p.execute()
        self.assertEqual(p._callbacks, [])
        self.assertEqual(p._stack, [])
        self.assertEqual(ref, 1)
        self.assertEqual(self.r.zrange('foo', 0, -1), [b'a'])

        p = redpipe.pipeline()
        ref = p.zadd('foo', 1, 'a')
        p.reset()
        p.execute()
        self.assertRaises(redpipe.ResultNotReady, lambda: ref.result)


class FieldsTestCase(unittest.TestCase):
    def test_float(self):
        field = redpipe.FloatField
        self.assertRaises(redpipe.InvalidValue, lambda: field.encode(''))
        self.assertRaises(redpipe.InvalidValue, lambda: field.encode('a'))
        self.assertRaises(redpipe.InvalidValue, lambda: field.encode('1'))
        self.assertRaises(redpipe.InvalidValue, lambda: field.encode([]))
        self.assertRaises(redpipe.InvalidValue, lambda: field.encode({}))
        self.assertEqual(field.encode(1), '1')
        self.assertEqual(field.encode(1.2), '1.2')
        self.assertEqual(field.encode(1.2345), '1.2345')
        self.assertEqual(field.decode('1'), 1)
        self.assertEqual(field.decode('1.2'), 1.2)
        self.assertEqual(field.decode('1.2345'), 1.2345)
        self.assertRaises(ValueError, lambda: field.decode('x'))

    def test_int(self):
        field = redpipe.IntegerField
        self.assertEqual(field.encode(0), '0')
        self.assertEqual(field.encode(2), '2')
        self.assertEqual(field.encode(123456), '123456')
        self.assertRaises(redpipe.InvalidValue, lambda: field.encode(''))
        self.assertRaises(redpipe.InvalidValue, lambda: field.encode('a'))
        self.assertRaises(redpipe.InvalidValue, lambda: field.encode('1'))
        self.assertRaises(redpipe.InvalidValue, lambda: field.encode(1.2))
        self.assertEqual(field.encode(1), '1')
        self.assertEqual(field.decode(b'1234'), 1234)
        self.assertRaises(ValueError, lambda: field.decode('x'))

    def test_text(self):
        field = redpipe.TextField
        self.assertRaises(redpipe.InvalidValue, lambda: field.encode(1))
        self.assertRaises(redpipe.InvalidValue,
                          lambda: field.encode(False))

        self.assertRaises(redpipe.InvalidValue,
                          lambda: field.encode(0.12345))

        self.assertRaises(redpipe.InvalidValue,
                          lambda: field.encode([]))

        self.assertEqual(field.encode('d'), b'd')
        self.assertEqual(field.encode(json.loads('"15\u00f8C"')),
                         b'15\xc3\xb8C')

        self.assertEqual(field.encode(''), b'')
        self.assertEqual(field.encode('a'), b'a')
        self.assertEqual(field.encode('1'), b'1')
        self.assertEqual(field.encode('1.2'), b'1.2')
        self.assertEqual(field.encode('abc123$!'), b'abc123$!')
        sample = json.loads('"15\u00f8C"')
        self.assertEqual(
            field.decode(field.encode(sample)),
            sample
        )

        self.assertEqual(
            field.decode(field.encode(utf8_sample)),
            utf8_sample
        )

    def test_ascii(self):
        field = redpipe.AsciiField
        self.assertRaises(redpipe.InvalidValue,
                          lambda: field.encode(1))
        self.assertRaises(redpipe.InvalidValue,
                          lambda: field.encode(False))
        self.assertRaises(redpipe.InvalidValue,
                          lambda: field.encode(0.1))

        self.assertEqual(field.encode(''), b'')
        self.assertEqual(field.encode('dddd'), b'dddd')

        self.assertRaises(redpipe.InvalidValue,
                          lambda: field.encode(json.loads('"15\u00f8C"')))

        self.assertEqual(field.encode('1'), b'1')
        self.assertEqual(field.encode('1.2'), b'1.2')
        self.assertEqual(field.encode('abc123$!'), b'abc123$!')
        sample = '#$%^&*()!@#aABc'
        self.assertEqual(
            field.decode(field.encode(sample)),
            sample
        )

    def test_binary(self):
        field = redpipe.BinaryField
        self.assertRaises(redpipe.InvalidValue,
                          lambda: field.encode(1))
        self.assertRaises(redpipe.InvalidValue,
                          lambda: field.encode(False))
        self.assertRaises(redpipe.InvalidValue,
                          lambda: field.encode(0.1))

        if six.PY3:
            self.assertRaises(redpipe.InvalidValue,
                              lambda: field.encode(''))

            self.assertRaises(redpipe.InvalidValue,
                              lambda: field.encode('dddd'))

        sample = json.loads('"15\u00f8C"')
        self.assertRaises(redpipe.InvalidValue,
                          lambda: field.encode(sample))

        self.assertEqual(field.encode(b'1'), b'1')
        self.assertEqual(field.encode(b'1.2'), b'1.2')
        self.assertEqual(field.encode(b'abc123$!'), b'abc123$!')
        sample = b'#$%^&*()!@#aABc'
        self.assertEqual(
            field.decode(field.encode(sample)),
            sample
        )
        self.assertEqual(
            field.decode(field.encode(sample)),
            sample
        )

        sample = uuid.uuid4().bytes
        self.assertEqual(
            field.decode(field.encode(sample)),
            sample
        )

    def test_list(self):
        field = redpipe.ListField

        self.assertRaises(redpipe.InvalidValue,
                          lambda: field.encode(1))
        self.assertRaises(redpipe.InvalidValue,
                          lambda: field.encode(False))
        self.assertRaises(redpipe.InvalidValue,
                          lambda: field.encode(0.1))
        self.assertRaises(redpipe.InvalidValue,
                          lambda: field.encode('ddd'))
        self.assertRaises(redpipe.InvalidValue,
                          lambda: field.encode({}))
        self.assertRaises(redpipe.InvalidValue,
                          lambda: field.encode({'a': 1}))

        self.assertEqual(field.encode([1]), b'[1]')

        data = ['a', 1]
        self.assertEqual(
            field.decode(field.encode(data)),
            data)

        self.assertEqual(field.decode(data), data)

    def test_dict(self):
        field = redpipe.DictField
        self.assertRaises(redpipe.InvalidValue,
                          lambda: field.encode(1))
        self.assertRaises(redpipe.InvalidValue,
                          lambda: field.encode(False))
        self.assertRaises(redpipe.InvalidValue,
                          lambda: field.encode(0.1))
        self.assertRaises(redpipe.InvalidValue,
                          lambda: field.encode('ddd'))
        self.assertRaises(redpipe.InvalidValue,
                          lambda: field.encode([]))
        self.assertRaises(redpipe.InvalidValue,
                          lambda: field.encode([1]))

        self.assertEqual(field.encode({'a': 1}), b'{"a": 1}')

        data = {'a': 1}
        self.assertEqual(
            field.decode(field.encode(data)),
            data)

        self.assertEqual(field.decode(data), data)

    def test_string_list(self):
        field = redpipe.StringListField
        self.assertRaises(redpipe.InvalidValue,
                          lambda: field.encode(1))
        self.assertRaises(redpipe.InvalidValue,
                          lambda: field.encode(False))
        self.assertRaises(redpipe.InvalidValue,
                          lambda: field.encode(0.1))
        self.assertRaises(redpipe.InvalidValue,
                          lambda: field.encode('ddd'))
        self.assertRaises(redpipe.InvalidValue,
                          lambda: field.encode([1]))
        self.assertRaises(redpipe.InvalidValue,
                          lambda: field.encode({}))
        self.assertRaises(redpipe.InvalidValue,
                          lambda: field.encode({'a': 1}))
        self.assertEqual(field.encode(['1']), b'1')
        data = ['a', 'b', 'c']
        self.assertEqual(
            field.decode(field.encode(data)),
            data)

        self.assertEqual(field.decode(data), data)
        self.assertIsNone(field.decode(b''))


class StructUser(redpipe.Struct):
    keyspace = 'U'
    fields = {
        'first_name': redpipe.TextField,
        'last_name': redpipe.TextField,
    }

    @property
    def name(self):
        names = [self.get('first_name', None), self.get('last_name', None)]
        return ' '.join([v for v in names if v is not None])


class StructTestCase(BaseTestCase):
    User = StructUser

    class UserWithPk(StructUser):
        key_name = 'user_id'

    class UserWithAttributes(StructUser):
        field_attr_on = True
        keyspace = 'U'
        fields = {
            'first_name': redpipe.TextField,
            'last_name': redpipe.TextField,
        }

    def test(self):
        u = self.User('1')
        self.assertFalse(u.persisted)
        self.assertEqual(dict(u), {'_key': '1'})
        u = self.UserWithAttributes({'_key': '1'})
        self.assertEqual(u.first_name, None)
        self.assertEqual(u.last_name, None)
        u.update({'first_name': 'Fred', 'last_name': 'Flintstone'})
        self.assertTrue(u.persisted)
        u = self.UserWithAttributes('1')
        self.assertTrue(u.persisted)
        self.assertEqual(u['first_name'], 'Fred')
        self.assertEqual(u.first_name, 'Fred')
        self.assertEqual(u.last_name, 'Flintstone')
        self.assertRaises(AttributeError, lambda: u.non_existent_field)
        self.assertIn('U', str(u))
        self.assertIn('1', str(u))
        self.assertEqual(u['last_name'], 'Flintstone')
        self.assertEqual('Fred Flintstone', u.name)

        u.remove(['last_name', 'test_field'])
        self.assertRaises(KeyError, lambda: u['last_name'])
        self.assertRaises(AttributeError, lambda: u.non_existent_field)
        u.update({'first_name': 'Wilma', 'arbitrary_field': 'a'})
        self.assertEqual(u['first_name'], 'Wilma')
        self.assertEqual(u.arbitrary_field, 'a')
        u = self.UserWithAttributes('1')
        core = self.User.core()
        self.assertTrue(core.exists('1'))
        self.assertEqual(u['first_name'], 'Wilma')
        self.assertEqual('Wilma', u['first_name'])

        with self.assertRaises(redpipe.InvalidOperation):
            u.first_name = 'test'

        with self.assertRaises(AttributeError):
            u.non_existent_field = 1

        with self.assertRaises(redpipe.InvalidOperation):
            u['first_name'] = 'a'

        with self.assertRaises(redpipe.InvalidOperation):
            del u['first_name']

        u_copy = dict(u)
        u_clone = self.User(u, no_op=True)
        u.clear()
        self.assertFalse(core.exists('1'))
        self.assertRaises(KeyError, lambda: u['first_name'])
        self.assertEqual(u_clone['first_name'], 'Wilma')
        self.assertFalse(u.persisted)
        u = self.User(u_copy)
        self.assertTrue(core.exists('1'))
        self.assertEqual(u['first_name'], 'Wilma')
        self.assertEqual(u['arbitrary_field'], 'a')
        self.assertIn('_key', u)
        self.assertEqual(u.key, '1')
        self.assertEqual(u['_key'], '1')
        self.assertEqual(repr(u), repr(dict(u)))
        self.assertEqual(json.dumps(u), json.dumps(dict(u)))
        u_pickled = pickle.loads(pickle.dumps(u))
        self.assertEqual(u_pickled, u)
        self.assertEqual(len(u), 3)
        self.assertIn('first_name', u)
        u.update({'first_name': 'Pebbles'})
        self.assertEqual(core.hget(u.key, 'first_name'), 'Pebbles')
        self.assertEqual(u['first_name'], 'Pebbles')
        self.assertEqual(dict(u.copy()), dict(u))
        self.assertEqual(u, dict(u))
        self.assertEqual(u, u)
        self.assertNotEqual(u, 1)
        self.assertNotEqual(u, u.keys())
        self.assertEqual({k for k in u}, set(u.keys()))
        u.remove(['arbitrary_field'])
        self.assertEqual(u.get('arbitrary_field'), None)
        self.assertEqual(core.hget(u.key, 'arbitrary_field'), None)
        u_pickled.update({'first_name': 'Dummy'})
        self.assertEqual(u_pickled['first_name'], 'Dummy')
        self.assertEqual(core.hget(u.key, 'first_name'), 'Dummy')

        u = self.User('1')
        with self.assertRaises(AttributeError):
            assert(u.first_name is None)

        with self.assertRaises(AttributeError):
            u.first_name = 'test'

    def fake_user_data(self, **kwargs):
        data = {
            'first_name': 'Bubba',
            'last_name': 'Jones',
            'email': 'bubbajones@fake.com',
        }
        data.update(kwargs)
        return data

    def test_empty_fields_init(self):

        class Test(redpipe.Struct):
            key_name = 't'
            default_fields = 'all'

        t = Test({'t': '1', 'orig': '1'})
        self.assertEqual(t, {'t': '1', 'orig': '1'})

        t = Test('1', fields=[])
        self.assertEqual(t, {'t': '1'})

        t = Test('1')
        self.assertEqual(t, {'t': '1', 'orig': '1'})

        t = Test({'t': '1', 'new': '1'}, fields=[])
        self.assertEqual(t, {'t': '1', 'new': '1'})

    def test_core(self):
        data = self.fake_user_data(_key='1')
        self.User(data)
        ref = self.User.core().hgetall('1')
        self.assertEqual(ref.result['first_name'], data['first_name'])

    def test_pipeline(self):
        user_ids = ["%s" % i for i in range(1, 3)]
        with redpipe.autoexec() as pipe:
            users = [self.User(self.fake_user_data(_key=i, b='123'), pipe=pipe)
                     for i in user_ids]
            self.assertEqual([u.persisted for u in users],
                             [False for _ in user_ids])
            retrieved_users = [self.User(i, pipe=pipe) for i in user_ids]

        # before executing the pipe (exiting the with block),
        # the data will not show as persisted.
        # once pipe execute happens, it is persisted.
        self.assertEqual(
            [u.persisted for u in users],
            [True for _ in user_ids])

        self.assertEqual(
            [u['b'] for u in retrieved_users],
            ['123' for _ in retrieved_users])

    def test_fields(self):
        class Multi(redpipe.Struct):
            keyspace = 'M'
            fields = {
                'boolean': redpipe.BooleanField,
                'integer': redpipe.IntegerField,
                'float': redpipe.FloatField,
                'text': redpipe.TextField,
            }

        data = {
            '_key': 'm1',
            'text': 'xyz',
            'integer': 5,
            'boolean': False,
            'float': 2.123}

        m = Multi(data)
        expected = {'_key': 'm1'}
        expected.update(data)
        self.assertEqual(dict(m), expected)
        self.assertEqual(m['boolean'], data['boolean'])
        self.assertEqual(m.get('boolean'), data['boolean'])
        self.assertEqual(m.get('non_existent', 'foo'), 'foo')
        self.assertEqual(m.get('non_existent'), None)
        data.update({'_key': 'm2'})
        m = Multi(data)
        self.assertEqual(dict(m), data)
        m = Multi('m2')
        self.assertEqual(dict(m), data)

        self.assertRaises(
            redpipe.InvalidValue,
            lambda: Multi({'_key': 'm3', 'text': 123}))

        self.assertRaises(
            redpipe.InvalidValue,
            lambda: Multi({'_key': 'm3', 'boolean': 'abc'}))

    def test_extra_fields(self):
        data = self.fake_user_data(_key='1', first_name='Bob',
                                   last_name='smith', nickname='BUBBA')
        u = self.User(data)
        u = self.User('1')
        self.assertEqual(u['_key'], '1')
        self.assertEqual(u['nickname'], 'BUBBA')
        self.assertEqual(u.get('nickname'), 'BUBBA')
        self.assertEqual(u.get('nonexistent', 'test'), 'test')
        self.assertRaises(KeyError, lambda: u['nonexistent'])

    def test_missing_fields(self):
        data = self.fake_user_data(_key='1', first_name='Bob')
        del data['last_name']
        u = self.User(data)
        u = self.User('1')
        self.assertRaises(KeyError, lambda: u['last_name'])

    def test_load_fields(self):
        data = self.fake_user_data(_key='1', first_name='Bob')
        u = self.User(data)
        u = self.User('1', fields=['first_name', 'non_existent_field'])

        self.assertEqual(u['first_name'], data['first_name'])
        self.assertRaises(KeyError, lambda: u['last_name'])
        self.assertRaises(KeyError, lambda: u['non_existent_field'])

    def test_set(self):
        data = self.fake_user_data(_key='1', first_name='Bob')
        del data['last_name']
        u = self.User(data)
        with redpipe.autoexec() as pipe:
            u.update({'first_name': 'Cool', 'last_name': 'Dude'}, pipe=pipe)

            self.assertEqual(u['first_name'], 'Bob')
            self.assertRaises(KeyError, lambda: u['last_name'])
        self.assertEqual(u['first_name'], 'Cool')
        self.assertEqual(u['last_name'], 'Dude')

    def test_remove_pk(self):
        data = self.fake_user_data(_key='1')
        u = self.User(data)
        self.assertRaises(redpipe.InvalidOperation,
                          lambda: u.remove(['_key']))
        self.assertRaises(redpipe.InvalidOperation,
                          lambda: u.update({'_key': '2'}))

    def test_custom_pk(self):
        data = self.fake_user_data(user_id='1')
        u = self.UserWithPk(data)
        self.assertEqual(u['user_id'], '1')
        self.assertIn('user_id', u)
        u_copy = self.UserWithPk(u, no_op=True)
        self.assertEqual(u_copy, u)

        u = self.UserWithPk('1')
        self.assertEqual(u_copy, u)

        with self.assertRaises(AttributeError):
            self.assertEqual(u.user_id, '1')

    def test_custom_pk_attr(self):
        class UserWithPkAttr(StructUser):
            key_name = 'user_id'
            field_attr_on = True

        data = self.fake_user_data(user_id='1')
        u = UserWithPkAttr(data)
        self.assertEqual(u.user_id, '1')

    def test_copy_with_no_pk(self):
        data = {'first_name': 'Bill'}
        self.assertRaises(redpipe.InvalidOperation,
                          lambda: self.User(data))
        self.assertRaises(redpipe.InvalidOperation,
                          lambda: self.UserWithPk(data))

    def test_incr(self):
        key = '1'

        class T(redpipe.Struct):
            keyspace = 'T'
            fields = {

            }

        field = 'arbitrary_field'
        t = T(key)
        t.incr(field)
        self.assertEqual(t[field], '1')
        with redpipe.autoexec() as pipe:
            t.incr(field, pipe=pipe)
            self.assertEqual(t[field], '1')

        self.assertEqual(t[field], '2')

        t.incr(field, 3)
        self.assertEqual(t[field], '5')

        t.decr(field)
        self.assertEqual(t[field], '4')
        t.decr(field, 2)
        self.assertEqual(t[field], '2')

    def test_typed_incr(self):
        key = '1'

        class T(redpipe.Struct):
            keyspace = 'T'
            fields = {
                'counter': redpipe.IntegerField
            }

        field = 'counter'
        t = T(key)
        t.incr(field)
        self.assertEqual(t[field], 1)
        with redpipe.autoexec() as pipe:
            t.incr(field, pipe=pipe)
            self.assertEqual(t[field], 1)

        self.assertEqual(t[field], 2)

        t.incr(field, 3)
        self.assertEqual(t[field], 5)

        t.decr(field)
        self.assertEqual(t[field], 4)
        t.decr(field, 2)
        self.assertEqual(t[field], 2)
        arbitrary_field = t.pop(field)
        self.assertEqual(arbitrary_field, 2)
        self.assertEqual(t.get(field), None)

    def test_delete(self):
        keys = ['1', '2', '3']
        for k in keys:
            data = self.fake_user_data(_key=k)
            self.User(data)

        for k in keys:
            u = self.User(k)
            self.assertTrue(u.persisted)

        self.User.delete(keys)
        for k in keys:
            u = self.User(k)
            self.assertFalse(u.persisted)

    def test_indirect_overlap_of_pk(self):
        key = '1'
        other_key = '2'
        data = self.fake_user_data(user_id=key)
        u = self.UserWithPk(data)
        u.core().hset(key, 'user_id', other_key)
        u = self.UserWithPk(key)
        self.assertEqual(dict(u)['user_id'], key)
        self.assertNotIn('user_id', u._data)  # noqa
        self.assertEqual(u.key, key)

    def test_update_with_none_future(self):
        f = redpipe.Future()
        f.set(None)
        data = self.fake_user_data(user_id='1')
        u = self.UserWithPk(data)
        u.update({'first_name': f})
        u = self.UserWithPk('1')
        self.assertRaises(KeyError, lambda: u['first_name'])

    def test_with_empty_update(self):
        class Test(redpipe.Struct):
            keyspace = 'U'
            fields = {
                'a': redpipe.TextField,
            }
            key_name = 'k'

        data = {'k': '1', 'a': 'foo', 'b': 'bar'}
        t = Test(data)
        t.update({})
        self.assertEqual(t, data)

    def test_fields_custom_default(self):
        class Test(redpipe.Struct):
            keyspace = 'U'
            fields = {
                'a': redpipe.TextField,
                'b': redpipe.TextField,
            }
            default_fields = ['a']
            key_name = 'k'

        data = {'k': '1', 'a': 'foo', 'b': 'bar'}
        t = Test(data)
        self.assertEqual(t, data)
        t = Test(data['k'])
        self.assertEqual(t, {'k': '1', 'a': 'foo'})
        t.load(['b'])
        self.assertEqual(t, data)
        t = Test(data['k'], fields='all')
        self.assertEqual(t, data)

    def test_fields_custom_default_defined_only(self):
        class Test(redpipe.Struct):
            keyspace = 'U'
            fields = {
                'a': redpipe.TextField,
                'b': redpipe.TextField,
            }
            default_fields = 'defined'
            key_name = 'k'

        data = {'k': '1', 'a': 'foo', 'b': 'bar', 'c': 'bazz'}
        t = Test(data)
        self.assertEqual(t, data)
        t = Test(data['k'])
        self.assertEqual(t, {'k': '1', 'a': 'foo', 'b': 'bar'})
        t.load(['c'])
        self.assertEqual(t, data)
        t = Test(data['k'], fields='all')
        self.assertEqual(t, data)

    def test_nx(self):
        class Test(redpipe.Struct):
            keyspace = 'U'
            fields = {
                'f1': redpipe.TextField,
                'f2': redpipe.TextField,
            }
            key_name = 'k'

        t = Test({'k': '1', 'f1': 'a'})
        self.assertEqual(t['f1'], 'a')
        t = Test({'k': '1', 'f1': 'b', 'f2': 'c'}, nx=True)
        self.assertEqual(t['f1'], 'a')
        self.assertEqual(t['f2'], 'c')

    def test_required_fields(self):
        class Test(redpipe.Struct):
            keyspace = 'U'
            fields = {
                'a': redpipe.IntegerField,
                'b': redpipe.IntegerField
            }
            required = set('b')

        t = Test({'_key': 'abc', 'b': 123})
        self.assertEqual(t['b'], 123)
        with self.assertRaises(redpipe.InvalidOperation):
            Test({'_key': 'abc', 'a': 123})  # Create obj w/o required field
        with self.assertRaises(redpipe.InvalidOperation):
            # Update obj removing a required field
            t.update({'a': 456, 'b': None})

        # Make sure the other fields did NOT update on the failed update
        self.assertIsNone(t.get('a', None))

        t.update({'a': 456, 'b': 789})  # Update required field of obj
        self.assertEqual(t['a'], 456)
        self.assertEqual(t['b'], 789)
        t.update({'a': None})  # Update non-required field of obj
        self.assertIsNone(t.get('a', None))

    def test_required_adding_later(self):
        class Test(redpipe.Struct):
            keyspace = 'U'
            fields = {
                'a': redpipe.IntegerField,
                'b': redpipe.IntegerField
            }

        class Test2(redpipe.Struct):
            required = {'new_required_field'}
            keyspace = 'U'
            fields = {
                'a': redpipe.IntegerField,
                'b': redpipe.IntegerField
            }

        Test({'_key': 'abc', 'b': 123})

        t = Test2('abc')
        t.update({'b': 456, 'random_field': 'hello_world'})
        self.assertEqual(t['b'], 456)
        self.assertEqual(t['random_field'], 'hello_world')
        self.assertIsNone(t.get('new_required_field', None))
        with self.assertRaises(redpipe.InvalidOperation):
            # Update obj removing a required field that didn't yet exist
            t.update({'a': 456, 'new_required_field': None})


class ConnectTestCase(unittest.TestCase):
    def tearDown(self):
        redpipe.reset()

    def incr_a(self, key, pipe=None):
        with redpipe.autoexec(pipe, name='a') as pipe:
            return pipe.incr(key)

    def incr_b(self, key, pipe=None):
        with redpipe.autoexec(pipe, name='b') as pipe:
            return pipe.incr(key)

    def test(self):
        r = redislite.StrictRedis()
        redpipe.connect_redis(r)
        redpipe.connect_redis(r)
        self.assertRaises(
            redpipe.AlreadyConnected,
            lambda: redpipe.connect_redis(redislite.StrictRedis()))
        redpipe.disconnect()
        redpipe.connect_redis(redislite.StrictRedis())

        # tear down the connection
        redpipe.disconnect()

        # calling it multiple times doesn't hurt anything
        redpipe.disconnect()

        redpipe.connect_redis(r)
        redpipe.connect_redis(
            redis.Redis(connection_pool=r.connection_pool))
        redpipe.connect_redis(r)

        self.assertRaises(
            redpipe.AlreadyConnected,
            lambda: redpipe.connect_redis(
                redislite.StrictRedis()))

    def test_with_decode_responses(self):
        def connect():
            redpipe.connect_redis(
                redislite.StrictRedis(decode_responses=True))

        self.assertRaises(redpipe.InvalidPipeline, connect)

    def test_single_nested(self):
        redpipe.connect_redis(redislite.StrictRedis(), 'a')

        def mid_level(pipe=None):
            with redpipe.autoexec(pipe, name='a') as pipe:
                return self.incr_a('foo', pipe=pipe)

        def top_level(pipe=None):
            with redpipe.autoexec(pipe, name='a') as pipe:
                return mid_level(pipe)

        with redpipe.autoexec(name='a') as pipe:
            ref = top_level(pipe)
            self.assertRaises(redpipe.ResultNotReady, lambda: ref.result)

        self.assertEqual(ref.result, 1)

    def test_sync(self):
        try:
            redpipe.disable_threads()
            self.test_single_nested()
            self.tearDown()
            self.test_pipeline_nested_mismatched_name()
            self.tearDown()
            self.test_multi_invalid_connection()
            self.tearDown()
            self.test_sleeping_cb()
        finally:
            redpipe.enable_threads()

    def test_sleeping_cb(self):
        redpipe.connect_redis(redislite.Redis(), 'a')
        redpipe.connect_redis(redislite.Redis(), 'b')

        with redpipe.autoexec(name='a') as pipe:
            pipe.set('foo', '1')
            with redpipe.autoexec(pipe=pipe, name='b') as p:
                ref = p.blpop('1', timeout=1)

        self.assertEqual(ref.result, None)

    def test_multi(self):

        a_conn = redislite.StrictRedis()
        b_conn = redislite.StrictRedis()
        redpipe.connect_redis(a_conn, name='a')
        redpipe.connect_redis(b_conn, name='b')

        key = 'foo'
        verify_callback = []
        with redpipe.pipeline() as pipe:
            a = self.incr_a(key, pipe)
            b = self.incr_b(key, pipe)

            def cb():
                verify_callback.append(1)

            pipe.on_execute(cb)
            pipe.execute()

        self.assertEqual(a.result, 1)
        self.assertEqual(b.result, 1)
        self.assertEqual(verify_callback, [1])

        # test failure
        try:
            with redpipe.autoexec() as pipe:
                a = self.incr_a(key, pipe)
                raise Exception('boo')
        except Exception:
            pass

        self.assertRaises(redpipe.ResultNotReady, lambda: a.result)

    def test_multi_auto(self):

        a_conn = redislite.StrictRedis()
        b_conn = redislite.StrictRedis()
        redpipe.connect_redis(a_conn)
        redpipe.connect_redis(a_conn, name='a')
        redpipe.connect_redis(b_conn, name='b')

        key = 'foo'
        verify_callback = []
        with redpipe.autoexec() as pipe:
            a = self.incr_a(key, pipe)
            b = self.incr_b(key, pipe)

            def cb():
                verify_callback.append(1)

            pipe.on_execute(cb)

        self.assertEqual(a.result, 1)
        self.assertEqual(b.result, 1)
        self.assertEqual(verify_callback, [1])

    def test_multi_invalid_connection(self):
        a_conn = redislite.StrictRedis()
        b_conn = redislite.StrictRedis(port=987654321)
        redpipe.connect_redis(a_conn, name='a')
        redpipe.connect_redis(b_conn, name='b')

        key = 'foo'
        verify_callback = []
        with redpipe.pipeline(name='a') as pipe:
            a = self.incr_a(key, pipe)
            b = self.incr_b(key, pipe)

            def cb():
                verify_callback.append(1)

            pipe.on_execute(cb)
            self.assertRaises(redis.ConnectionError, pipe.execute)

        # you can see here that it's not a 2-phase commit.
        # the goal is not tranactional integrity.
        # it is parallel execution of network tasks.
        self.assertRaises(redpipe.ResultNotReady, lambda: a.result)
        self.assertRaises(redpipe.ResultNotReady, lambda: b.result)
        self.assertEqual(verify_callback, [])

    def test_pipeline_mismatched_name(self):
        a_conn = redislite.StrictRedis()
        b_conn = redislite.StrictRedis()
        redpipe.connect_redis(a_conn, name='a')
        redpipe.connect_redis(b_conn, name='b')

        with redpipe.pipeline(name='b') as pipe:
            ref = self.incr_a(key='foo', pipe=pipe)
            self.assertRaises(redpipe.ResultNotReady, lambda: ref.result)
            pipe.execute()

    def test_pipeline_nested_mismatched_name(self):
        a_conn = redislite.StrictRedis()
        b_conn = redislite.StrictRedis()
        redpipe.connect_redis(a_conn, name='a')
        redpipe.connect_redis(b_conn, name='b')

        def my_function(pipe=None):
            with redpipe.pipeline(pipe=pipe, name='b') as pipe:
                ref = self.incr_a(key='foo', pipe=pipe)
                self.assertRaises(redpipe.ResultNotReady, lambda: ref.result)
                pipe.execute()
                return ref

        with redpipe.pipeline(name='a') as pipe:
            ref1 = my_function(pipe=pipe)
            ref2 = my_function(pipe=pipe)
            self.assertRaises(redpipe.ResultNotReady, lambda: ref1.result)
            self.assertRaises(redpipe.ResultNotReady, lambda: ref2.result)
            pipe.execute()
        self.assertEqual(ref1.result, 1)
        self.assertEqual(ref2.result, 2)

    def test_pipeline_invalid_object(self):
        a_conn = redislite.StrictRedis()
        b_conn = redislite.StrictRedis()
        redpipe.connect_redis(a_conn)
        redpipe.connect_redis(a_conn, name='a')
        redpipe.connect_redis(b_conn, name='b')

        def do_invalid():
            self.incr_a(key='foo', pipe='invalid')

        self.assertRaises(redpipe.InvalidPipeline, do_invalid)

    def test_unconfigured_pipeline(self):

        def invalid():
            self.incr_a(key='foo')

        def nested_invalid():
            with redpipe.autoexec() as pipe:
                self.incr_a(key='foo', pipe=pipe)

        self.assertRaises(redpipe.InvalidPipeline, invalid)
        self.assertRaises(redpipe.InvalidPipeline, nested_invalid)


class ConnectRedisClusterTestCase(unittest.TestCase):
    def tearDown(self):
        redpipe.reset()

    def test(self):
        # i don't need to set up a full cluster to test. this.
        # it's enough to know I wired it into the code correctly for now.
        r = rediscluster.StrictRedisCluster(
            startup_nodes=[{'host': '0', 'port': 999999}],
            init_slot_cache=False
        )
        redpipe.connect_redis(r, 'test')
        with redpipe.pipeline(name='test') as pipe:
            pipe.set('foo', 'bar')
            self.assertRaises(Exception, pipe.execute)


class RedisClusterTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.c = SingleNodeRedisCluster(strict=False)
        cls.r = cls.c.client
        redpipe.connect_redis(cls.r)

    @classmethod
    def tearDownClass(cls):
        cls.r = None
        cls.c.shutdown()
        cls.c = None
        redpipe.reset()

    def tearDown(self):
        self.r.flushall()

    def test_basic(self):
        with redpipe.autoexec() as pipe:
            pipe.set('foo', 'bar')
            res = pipe.get('foo')
        self.assertEqual(res, b'bar')

    def test_list(self):
        class Test(redpipe.List):
            keyspace = 'T'

        with redpipe.autoexec() as pipe:
            t = Test(pipe)
            append = t.rpush('1', 'a', 'b', 'c')
            lrange = t.lrange('1', 0, -1)
            lpop = t.lpop('1')

        self.assertEqual(append, 3)
        self.assertEqual(lrange, ['a', 'b', 'c'])
        self.assertEqual(lpop, 'a')

    def test_set(self):
        class Test(redpipe.Set):
            keyspace = 'T'

        with redpipe.autoexec() as pipe:
            t = Test(pipe)
            sadd = t.sadd('1', 'a', 'b', 'c')
            smembers = t.smembers('1')
            spop = t.spop('1')
            scard = t.scard('1')

        expected = {'a', 'b', 'c'}
        self.assertEqual(sadd, 3)
        self.assertEqual(smembers, expected)
        self.assertIn(spop, expected)
        self.assertEqual(scard, 2)

    def test_string(self):
        class Test(redpipe.String):
            keyspace = 'T'

        with redpipe.autoexec() as pipe:
            t = Test(pipe)
            set_result = t.set('1', 'a')
            get_result = t.get('1')
            delete_result = t.delete('1')

        self.assertEqual(set_result, 1)
        self.assertEqual(get_result, 'a')
        self.assertEqual(delete_result, 1)

    def test_sorted_sets(self):
        class Test(redpipe.SortedSet):
            keyspace = 'T'

        with redpipe.autoexec() as pipe:
            t = Test(pipe)
            t.zadd('1', 'a', 1)
            t.zadd('1', 'b', 2)
            zadd = t.zadd('1', 'c', 3)
            zrange = t.zrange('1', 0, -1)
            zincrby = t.zincrby('1', 'a', 1)

        self.assertEqual(zadd, 1)
        self.assertEqual(zrange, ['a', 'b', 'c'])
        self.assertEqual(zincrby, 2)

    def test_hll_commands(self):
        class Test(redpipe.HyperLogLog):
            keyspace = 'T'

        with redpipe.autoexec() as pipe:
            t = Test(pipe)
            pfadd = t.pfadd('1', 'a', 'b', 'c')
            t.pfadd('1', 'a', 'b', 'c')
            t.pfadd('1', 'd')
            pfcount = t.pfcount('1')

        self.assertEqual(pfadd, 1)
        self.assertEqual(pfcount, 4)


class StrictRedisClusterTestCase(RedisClusterTestCase):
    @classmethod
    def setUpClass(cls):
        cls.c = SingleNodeRedisCluster(strict=True)
        cls.r = cls.c.client
        redpipe.connect_redis(cls.r)

    @classmethod
    def tearDownClass(cls):
        cls.r = None
        cls.c.shutdown()
        cls.c = None
        redpipe.reset()


class StrictStringTestCase(BaseTestCase):
    class Data(redpipe.String):
        keyspace = 'STRING'

    def test(self):
        with redpipe.autoexec() as pipe:
            key = '1'
            s = self.Data(pipe=pipe)
            self.assertEqual(s.redis_key(key), b'STRING{1}')
            s.set(key, '2')
            before = s.get(key)
            mget_res = s.mget([key])
            serialize = s.dump(key)
            s.expire(key, 3)
            ttl = s.ttl(key)
            s.delete(key)
            exists = s.exists(key)
            after = s.get(key)
            self.assertRaises(redpipe.ResultNotReady, lambda: before.result)

        self.assertEqual(before, '2')
        self.assertEqual(['2'], mget_res)
        self.assertEqual(after, None)
        self.assertAlmostEqual(ttl, 3, delta=1)
        self.assertIsNotNone(serialize.result)
        self.assertFalse(exists.result)

        with redpipe.autoexec() as pipe:
            key = '2'
            s = self.Data(pipe=pipe)
            restore = s.restore(key, serialize.result)
            restorenx = s.restorenx(key, serialize.result)
            ref = s.get(key)
            idle = s.object('IDLETIME', key)
            persist = s.persist(key)
            incr = s.incr(key)
            incrby = s.incrby(key, 2)
            incrbyfloat = s.incrbyfloat(key, 2.1)
            setnx = s.setnx(key, 'foo')
            getaftersetnx = s.get(key)
            setex = s.setex(key, 'bar', 60)
            getaftersetex = s.get(key)
            ttl = s.ttl(key)
            psetex = s.psetex(key, 'bar', 6000)
        self.assertEqual(restore.result, 'OK')
        self.assertEqual(restorenx.result, 0)
        self.assertEqual(ref, '2')
        self.assertEqual(str(s), '<Data>')
        self.assertEqual(idle, 0)
        self.assertEqual(persist, 0)
        self.assertEqual(incr, 3)
        self.assertEqual(incrby.result, 5)
        self.assertEqual(incrbyfloat.result, 7.1)
        self.assertEqual(setnx.result, 0)
        self.assertEqual(getaftersetnx.result, '7.1')
        self.assertEqual(setex, 1)
        self.assertEqual(getaftersetex, 'bar')
        self.assertAlmostEqual(ttl, 60, delta=1)
        self.assertEqual(psetex, 1)

        with redpipe.autoexec() as pipe:
            key = '3'
            s = self.Data(pipe=pipe)
            s.set(key, 'bar')
            append = s.append(key, 'r')
            substr = s.substr(key, 1, 3)
            strlen = s.strlen(key)
            setrange = s.setrange(key, 1, 'azz')
            get = s.get(key)

        self.assertEqual(append, 4)
        self.assertEqual(strlen, 4)
        self.assertEqual(substr, 'arr')
        self.assertEqual(setrange, 4)
        self.assertEqual(get, 'bazz')

    def test_bitwise(self):
        with redpipe.autoexec() as pipe:
            key = '1'
            s = self.Data(pipe=pipe)
            setbit = s.setbit(key, 2, 1)
            getbit = s.getbit(key, 2)
            bitcount = s.bitcount(key)

        self.assertEqual(setbit, 0)
        self.assertEqual(getbit, 1)
        self.assertEqual(bitcount, 1)

    def test_rename(self):
        with redpipe.autoexec() as pipe:
            key1 = '1'
            key2 = '2'
            key3 = '3'
            s = self.Data(pipe=pipe)
            s.set(key1, '1')
            rename = s.rename(key1, key2)

            s.set(key3, '3')
            renamenx = s.renamenx(key3, key2)
            get = s.get(key2)

        self.assertEqual(rename, 1)
        self.assertEqual(renamenx, 0)
        self.assertEqual(get, '1')

    def test_dict(self):
        key1 = '1'
        with redpipe.autoexec() as pipe:
            s = self.Data(pipe=pipe)
            s[key1] = 'a'
            get = s[key1]
        self.assertEqual(get, 'a')

    def test_bare(self):
        with redpipe.autoexec() as pipe:
            key = 'foo'
            f = redpipe.String(pipe=pipe)
            self.assertEqual(f.redis_key(key), b'foo')
            f.set(key, '2')
            before = f.get(key)
            serialize = f.dump(key)
            f.expire(key, 3)
            ttl = f.ttl(key)
            pttl = f.pttl(key)
            pexpire = f.pexpire(key, 3000)
            pexpireat = f.pexpireat(key, int(time.time() * 1000) + 1000)

            f.delete(key)
            exists = f.exists(key)
            after = f.get(key)
            self.assertRaises(redpipe.ResultNotReady, lambda: before.result)
        self.assertEqual(before.result, '2')
        self.assertEqual(after.result, None)
        self.assertAlmostEqual(ttl.result, 3, delta=1)
        self.assertAlmostEqual(pttl, 3000, delta=100)
        self.assertEqual(pexpire, 1)
        self.assertEqual(pexpireat, 1)
        self.assertIsNotNone(serialize.result)
        self.assertFalse(exists.result)

    def test_eval(self):
        key1 = '1'
        script = """return redis.call("SET", KEYS[1], ARGV[1])"""
        with redpipe.autoexec() as pipe:
            s = self.Data(pipe=pipe)
            s.eval(script, 1, key1, 'a')
            get = s.get(key1)
        self.assertEqual(get, 'a')


class StringTestCase(StrictStringTestCase):
    @classmethod
    def setUpClass(cls):
        cls.r = redislite.Redis()
        redpipe.connect_redis(cls.r)


class StrictSetTestCase(BaseTestCase):
    class Data(redpipe.Set):
        keyspace = 'SET'

    def test(self):
        with redpipe.autoexec() as pipe:
            key = '1'
            c = self.Data(pipe=pipe)
            sadd = c.sadd(key, ['a', 'b', 'c'])
            saddnx = c.sadd(key, 'a')
            srem = c.srem(key, 'c')
            smembers = c.smembers(key)
            card = c.scard(key)
            ismember_a = c.sismember(key, 'a')
            c.srem(key, 'b')
            ismember_b = c.sismember(key, 'b')
            srandmember = c.srandmember(key)
            srandmembers = c.srandmember(key, number=2)
            spop = c.spop(key)

        self.assertEqual(sadd.result, 3)
        self.assertEqual(saddnx.result, 0)
        self.assertEqual(srem.result, 1)
        self.assertEqual(smembers.result, {'a', 'b'})
        self.assertIn(spop.result, {'a', 'b'})
        self.assertEqual(card.result, 2)
        self.assertTrue(ismember_a.result)
        self.assertFalse(ismember_b.result)
        self.assertTrue(srandmember.result, b'a')
        self.assertTrue(srandmembers.result, [b'a'])

    def test_scan(self):
        with redpipe.autoexec() as pipe:
            key = '1'
            s = self.Data(pipe=pipe)
            s.sadd(key, 'a1', 'a2', 'b1', 'b2')
            sscan = s.sscan(key, 0, match='a*')

        self.assertEqual(sscan[0], 0)
        self.assertEqual(set(sscan[1]), {'a1', 'a2'})

        with redpipe.autoexec() as pipe:
            self.assertRaises(
                redpipe.InvalidOperation,
                lambda: {k for k in self.Data(pipe=pipe).sscan_iter('1')})

        data = {k for k in self.Data().sscan_iter('1')}
        self.assertEqual(data, {'a1', 'a2', 'b1', 'b2'})

    def test_sdiff(self):
        key1 = '1'
        key2 = '2'
        key3 = '3'
        with redpipe.autoexec() as pipe:
            s = self.Data(pipe=pipe)
            s.sadd(key1, 'a', 'b', 'c')
            s.sadd(key2, 'a', 'b', 'd', 'e')
            sdiff = s.sdiff(key2, key1)
            sinter = s.sinter(key1, key2)
            sinter_missing = s.sinter(key3, key2)
            sdiffstore = s.sdiffstore(key3, key2, key1)
            sdiffstore_get = s.smembers(key3)
            sinterstore = s.sinterstore(key3, key2, key1)
            sinterstore_get = s.smembers(key3)
            sunion = s.sunion(key1, key2)
            sunionstore = s.sunionstore(key3, key1, key2)
            sunionstore_get = s.smembers(key3)

        self.assertEqual(sdiff, {'e', 'd'})
        self.assertEqual(sinter, {'a', 'b'})
        self.assertEqual(sinter_missing, set())
        self.assertEqual(sdiffstore, 2)
        self.assertEqual(sdiffstore_get, {'e', 'd'})
        self.assertEqual(sinterstore, 2)
        self.assertEqual(sinterstore_get, {'a', 'b'})
        self.assertEqual(sunion, {'a', 'b', 'c', 'd', 'e'})
        self.assertEqual(sunionstore, 5)
        self.assertEqual(sunionstore_get, {'a', 'b', 'c', 'd', 'e'})


class SetTestCase(StrictSetTestCase):
    @classmethod
    def setUpClass(cls):
        cls.r = redislite.Redis()
        redpipe.connect_redis(cls.r)


class StrictListTestCase(BaseTestCase):
    class Data(redpipe.List):
        keyspace = 'LIST'

    def test(self):
        with redpipe.autoexec() as pipe:
            key = '1'
            c = self.Data(pipe=pipe)
            lpush = c.lpush(key, 'a', 'b', 'c', 'd')
            members = c.lrange(key, 0, -1)
            rpush = c.rpush(key, 'e')
            llen = c.llen(key, )
            lrange = c.lrange(key, 0, -1)
            rpop = c.rpop(key)
            lrem = c.lrem(key, 'a', 1)
            ltrim = c.ltrim(key, 0, 1)
            members_after_ltrim = c.lrange(key, 0, -1)
            lindex = c.lindex(key, 1)
            lset = c.lset(key, 1, 'a')
            lindex_after = c.lindex(key, 1)
            lpop = c.lpop(key)

        self.assertEqual(lpush.result, 4)
        self.assertEqual(members.result, ['d', 'c', 'b', 'a'])
        self.assertEqual(rpush.result, 5)
        self.assertEqual(llen.result, 5)
        self.assertEqual(lrange.result, ['d', 'c', 'b', 'a', 'e'])
        self.assertEqual(rpop.result, 'e')
        self.assertEqual(lrem.result, 1)
        self.assertEqual(ltrim.result, 1)
        self.assertEqual(members_after_ltrim.result, ['d', 'c'])
        self.assertEqual(lindex.result, 'c')
        self.assertEqual(lset.result, 1)
        self.assertEqual(lindex_after.result, 'a')
        self.assertEqual(lpop.result, 'd')

    def test_scan(self):
        with redpipe.autoexec() as pipe:
            d = self.Data(pipe=pipe)
            d.lpush('1a', '1')
            d.lpush('1b', '1')
            d.lpush('2a', '1')
            d.lpush('2b', '1')
            sscan = d.scan(0, match='1*')
            sscan_all = d.scan()

        self.assertEqual(sscan[0], 0)
        self.assertEqual(set(sscan[1]), {'1a', '1b'})
        self.assertEqual(set(sscan_all[1]), {'1a', '1b', '2a', '2b'})
        self.assertEqual({k for k in self.Data().scan_iter()},
                         {'1a', '1b', '2a', '2b'})

        with redpipe.autoexec() as pipe:
            s = self.Data(pipe=pipe)
            self.assertRaises(redpipe.InvalidOperation,
                              lambda: [v for v in s.scan_iter()])

    def test_scan_with_no_keyspace(self):
        with redpipe.autoexec() as pipe:
            t = redpipe.List(pipe=pipe)
            t.lpush('1a', '1')
            t.lpush('1b', '1')
            t.lpush('2a', '1')
            t.lpush('2b', '1')
            sscan = t.scan(0, match='1*')

        self.assertEqual(sscan[0], 0)
        self.assertEqual(set(sscan[1]), {'1a', '1b'})

    def test_pop(self):
        key1 = '1'
        key2 = '2'
        key3 = '3'
        with redpipe.autoexec() as pipe:
            t = self.Data(pipe=pipe)
            t.rpush(key1, 'a', 'b')
            t.rpush(key2, 'c', 'd')
            blpop = t.blpop([key1])
            brpop = t.brpop([key1])
            blpop_missing = t.blpop(['4', '5'], timeout=1)
            brpop_missing = t.brpop(['4', '5'], timeout=1)
            brpoplpush = t.brpoplpush(key2, key3, timeout=1)
            rpoplpush = t.rpoplpush(key2, key3)
            members = t.lrange(key3, 0, -1)

        self.assertEqual(blpop, ('1', 'a'))
        self.assertEqual(brpop, ('1', 'b'))
        self.assertEqual(blpop_missing, None)
        self.assertEqual(brpop_missing, None)
        self.assertEqual(brpoplpush, 'd')
        self.assertEqual(rpoplpush, 'c')
        self.assertEqual(members, ['c', 'd'])


class ListTestCase(StrictListTestCase):
    @classmethod
    def setUpClass(cls):
        cls.r = redislite.Redis()
        redpipe.connect_redis(cls.r)


class StrictSortedSetTestCase(BaseTestCase):
    class Data(redpipe.SortedSet):
        keyspace = 'SORTEDSET'

    def test(self):
        with redpipe.autoexec() as pipe:
            key = '1'
            s = self.Data(pipe=pipe)
            s.zadd(key, '2', 2)
            s.zadd(key, '3', 3)
            add = s.zadd(key, '4', 4)
            zaddincr = s.zadd(key, '4', 1, incr=True)
            zscore_after_incr = s.zscore(key, '4')
            zaddnx = s.zadd(key, '4', 4.1, nx=True)
            zaddxx = s.zadd(key, '4', 4.2, xx=True)
            zaddch = s.zadd(key, '4', 4.3, ch=True)
            zscore = s.zscore(key, '4')
            remove = s.zrem(key, '4')
            members = s.zrange(key, 0, -1)
            zaddmulti = s.zadd(key, {'4': 4, '5': 5})
            zincrby = s.zincrby(key, '5', 2)
            zrevrank = s.zrevrank(key, '5')
            zrevrange = s.zrevrange(key, 0, 1)
            zrange_withscores = s.zrange(key, 0, 1, withscores=True)
            zrevrange_withscores = s.zrevrange(key, 0, 1, withscores=True)

            self.assertRaises(
                redpipe.InvalidOperation,
                lambda: s.zadd(key, '4', 4, xx=True, nx=True))
            s.delete(key)
            zrange = s.zrange(key, 0, -1)
            self.assertRaises(redpipe.ResultNotReady, lambda: members.result)
        self.assertEqual(add.result, 1)
        self.assertEqual(zaddincr.result, 5)
        self.assertEqual(zscore_after_incr.result, 5)
        self.assertEqual(zaddnx, 0)
        self.assertEqual(zaddxx, 0)
        self.assertEqual(zaddch, 1)
        self.assertEqual(zscore, 4.3)
        self.assertEqual(remove, 1)
        self.assertEqual(members, ['2', '3'])
        self.assertEqual(zaddmulti, 2)
        self.assertEqual(zrange, [])
        self.assertEqual(zincrby, 7.0)
        self.assertEqual(zrevrank, 0)
        self.assertEqual(zrevrange, ['5', '4'])
        self.assertEqual(zrange_withscores, [('2', 2.0), ('3', 3.0)])
        self.assertEqual(zrevrange_withscores, [('5', 7.0), ('4', 4.0)])

        with redpipe.autoexec() as pipe:
            key = '1'
            s = self.Data(pipe=pipe)
            s.zadd(key, 'a', 1)
            s.zadd(key, 'b', 2)
            zrangebyscore = s.zrangebyscore(key, 0, 10, start=0, num=1)
            zrangebyscore_withscores = s.zrangebyscore(key,
                                                       0, 10, start=0, num=1,
                                                       withscores=True)
            zrevrangebyscore = s.zrevrangebyscore(key, 10, 0, start=0, num=1)
            zrevrangebyscore_withscores = s.zrevrangebyscore(key,
                                                             10, 0, start=0,
                                                             num=1,
                                                             withscores=True)
            zcard = s.zcard(key)
            zcount = s.zcount(key, '-inf', '+inf')
            zrank = s.zrank(key, 'b')
            zlexcount = s.zlexcount(key, '-', '+')
            zrangebylex = s.zrangebylex(key, '-', '+')
            zrevrangebylex = s.zrevrangebylex(key, '+', '-')
            zremrangebyrank = s.zremrangebyrank(key, 0, 0)
            zremrangebyscore = s.zremrangebyscore(key, 2, 2)
            zremrangebylex = s.zremrangebylex(key, '-', '+')

        self.assertEqual(zrangebyscore, ['a'])
        self.assertEqual(zrangebyscore_withscores, [('a', 1.0)])
        self.assertEqual(zrevrangebyscore, ['b'])
        self.assertEqual(zrevrangebyscore_withscores, [('b', 2.0)])
        self.assertEqual(zcard, 2)
        self.assertEqual(zcount, 2)
        self.assertEqual(zrank, 1)
        self.assertEqual(zremrangebyrank, 1)
        self.assertEqual(zremrangebyscore, 1)
        self.assertEqual(zlexcount, 2)
        self.assertEqual(zrangebylex, ['a', 'b'])
        self.assertEqual(zrevrangebylex, ['b', 'a'])
        self.assertEqual(zremrangebylex, 0)

    def test_scan(self):
        with redpipe.autoexec() as pipe:
            key = '1'
            s = self.Data(pipe=pipe)
            s.zadd(key, 'a1', 1.0)
            s.zadd(key, 'a2', 2)
            s.zadd(key, 'b1', 1)
            s.zadd(key, 'b2', 2)
            sscan = s.zscan(key, 0, match='a*')
            sort = s.sort(key, alpha=True)
            sort_store = s.sort(key, alpha=True, store=True)

        self.assertEqual(sscan[0], 0)
        self.assertEqual(set(sscan[1]), {('a1', 1.0), ('a2', 2.0)})
        self.assertEqual(sort, ['a1', 'a2', 'b1', 'b2'])
        self.assertEqual(sort_store, 4)

        with redpipe.autoexec() as pipe:
            self.assertRaises(
                redpipe.InvalidOperation,
                lambda: {k for k in self.Data(pipe=pipe).zscan_iter(key)})

        data = {k for k in self.Data().zscan_iter(key)}
        expected = {('a1', 1.0), ('a2', 2.0), ('b1', 1.0), ('b2', 2.0)}
        self.assertEqual(data, expected)

    def test_union(self):
        key1 = '1'
        key2 = '2'
        key3 = '3'
        with redpipe.autoexec() as pipe:
            s = self.Data(pipe=pipe)
            s.zadd(key1, 'a', 1)
            s.zadd(key1, 'b', 2)
            s.zadd(key2, 'c', 3)
            s.zadd(key2, 'd', 4)
            zunionstore = s.zunionstore(key3, [key1, key2])
            zrange = s.zrange(key3, 0, -1)

        self.assertEqual(zunionstore, 4)
        self.assertEqual(zrange, ['a', 'b', 'c', 'd'])


class SortedSetTestCase(StrictSortedSetTestCase):
    @classmethod
    def setUpClass(cls):
        cls.r = redislite.Redis()
        redpipe.connect_redis(cls.r)


class StrictHashTestCase(BaseTestCase):
    class Data(redpipe.Hash):
        keyspace = 'HASH'

    def test(self):
        with redpipe.autoexec() as pipe:
            key = '1'
            c = self.Data(pipe=pipe)
            hset = c.hset(key, 'a', '1')
            hmset = c.hmset(key, {'b': '2', 'c': '3', 'd': '4'})
            hsetnx = c.hsetnx(key, 'b', '9999')
            hget = c.hget(key, 'a')
            hgetall = c.hgetall(key, )
            hlen = c.hlen(key, )
            hdel = c.hdel(key, 'a', 'b')
            hkeys = c.hkeys(key)
            hexists = c.hexists(key, 'c')
            hincrby = c.hincrby(key, 'd', 2)
            hmget = c.hmget(key, ['c', 'd'])
            hvals = c.hvals(key)

        self.assertEqual(hset.result, True)
        self.assertEqual(hmset.result, True)
        self.assertEqual(hsetnx.result, 0)
        self.assertEqual(hget.result, '1')
        self.assertEqual(
            hgetall.result,
            {'a': '1', 'd': '4', 'b': '2', 'c': '3'})
        self.assertEqual(hlen.result, 4)
        self.assertEqual(hdel.result, 2)
        self.assertEqual(set(hkeys.result), {'c', 'd'})
        self.assertTrue(hexists.result)
        self.assertEqual(hincrby.result, 6)
        self.assertEqual(hmget.result, ['3', '6'])
        self.assertEqual(set(hvals.result), {'3', '6'})

    def test_scan(self):
        key = '1'
        with redpipe.autoexec() as pipe:
            s = self.Data(pipe=pipe)
            s.hmset(key, {'a1': '1', 'a2': '2', 'b1': '1', 'b2': '2'})
            hscan = s.hscan(key, 0, match='a*')

        self.assertEqual(hscan[0], 0)
        self.assertEqual(hscan[1], {'a1': '1', 'a2': '2'})

        with redpipe.autoexec() as pipe:
            s = self.Data(pipe=pipe)
            self.assertRaises(redpipe.InvalidOperation,
                              lambda: [v for v in s.hscan_iter(key)])

        data = {k: v for k, v in self.Data().hscan_iter(key)}
        self.assertEqual(data, {'b2': '2', 'b1': '1', 'a1': '1', 'a2': '2'})


class HashTestCase(StrictHashTestCase):
    @classmethod
    def setUpClass(cls):
        cls.r = redislite.Redis()
        redpipe.connect_redis(cls.r)


class HashFieldsTestCase(BaseTestCase):
    class Data(redpipe.Hash):
        keyspace = 'HASH'
        fields = {
            'b': redpipe.BooleanField,
            'i': redpipe.IntegerField,
            'f': redpipe.FloatField,
            't': redpipe.TextField,
            'l': redpipe.ListField,
            'd': redpipe.DictField,
            'sl': redpipe.StringListField,
        }

    def test(self):
        key = '1'
        with redpipe.autoexec() as pipe:
            c = self.Data(pipe=pipe)
            hset = c.hset(key, 'i', 1)
            hmset = c.hmset(key, {'b': True, 'f': 3.1, 't': utf8_sample})
            hsetnx = c.hsetnx(key, 'b', False)
            hget = c.hget(key, 'b')
            hgetall = c.hgetall(key)
            hincrby = c.hincrby(key, 'i', 2)
            hincrbyfloat = c.hincrbyfloat(key, 'f', 2.1)
            hmget = c.hmget(key, ['f', 'b'])

        self.assertEqual(hset, True)
        self.assertEqual(hmset, True)
        self.assertEqual(hsetnx.result, 0)
        self.assertEqual(hget, True)
        self.assertEqual(
            hgetall.result,
            {'b': True, 'i': 1, 'f': 3.1, 't': utf8_sample})
        self.assertEqual(hincrby.result, 3)
        self.assertEqual(hincrbyfloat.result, 5.2)
        self.assertEqual(hmget.result, [5.2, True])

    def test_invalid_value(self):
        key = '1'
        with redpipe.pipeline() as pipe:
            c = self.Data(pipe=pipe)
            self.assertRaises(
                redpipe.InvalidValue, lambda: c.hset(key, 'i', 'a'))
            self.assertRaises(
                redpipe.InvalidValue, lambda: c.hset(key, 'b', '1'))
            self.assertRaises(
                redpipe.InvalidValue, lambda: c.hset(key, 't', 1))

            c.hset(key, 'f', 1)

            self.assertRaises(
                redpipe.InvalidValue, lambda: c.hset(key, 'f', '1'))

    def test_dict(self):
        key = 'd'
        with redpipe.autoexec() as pipe:
            data = {'a': 1, 'b': 'test'}
            c = self.Data(pipe=pipe)
            hset = c.hset(key, 'd', data)
            hget = c.hget(key, 'd')
            hmget = c.hmget(key, ['d'])
            hgetall = c.hgetall(key, )

        self.assertEqual(hset, 1)
        self.assertEqual(hget, data)
        self.assertEqual(hmget, [data])
        self.assertEqual(hgetall, {'d': data})

    def test_list(self):
        key = '1'
        with redpipe.autoexec() as pipe:
            data = [1, 'a']
            c = self.Data(pipe=pipe)
            hset = c.hset(key, 'l', data)
            hget = c.hget(key, 'l')
            hmget = c.hmget(key, ['l'])
            hgetall = c.hgetall(key, )

        self.assertEqual(hset, 1)
        self.assertEqual(hget, data)
        self.assertEqual(hmget, [data])
        self.assertEqual(hgetall, {'l': data})

    def test_string_list(self):
        key = '1'
        with redpipe.autoexec() as pipe:
            data = ['a', 'b']
            c = self.Data(pipe=pipe)
            hget_pre = c.hget(key, 'sl')
            hset = c.hset(key, 'sl', data)
            hget = c.hget(key, 'sl')
            hmget = c.hmget(key, ['sl'])
            hgetall = c.hgetall(key, )

        self.assertEqual(hget_pre, None)
        self.assertEqual(hset, 1)
        self.assertEqual(hget, data)
        self.assertEqual(hmget, [data])
        self.assertEqual(hgetall, {'sl': data})


class HyperloglogTestCase(BaseTestCase):
    class Data(redpipe.HyperLogLog):
        keyspace = 'HYPERLOGLOG'

    def test(self):
        key1 = '1'
        key2 = '2'
        key3 = '3'
        with redpipe.autoexec() as pipe:
            c = self.Data(pipe=pipe)
            pfadd = c.pfadd(key1, 'a', 'b', 'c')
            pfcount = c.pfcount(key1)
            c.pfadd(key2, 'b', 'c', 'd')
            pfmerge = c.pfmerge(key3, key1, key2)
            pfcount_aftermerge = c.pfcount(key3)

        self.assertEqual(pfadd, 1)
        self.assertEqual(pfcount, 3)
        self.assertEqual(pfmerge, True)
        self.assertEqual(pfcount_aftermerge, 4)


class AsyncTestCase(unittest.TestCase):
    def test(self):
        def sleeper():
            time.sleep(0.3)
            return 1

        t = redpipe.tasks.AsynchronousTask(target=sleeper)
        t.start()
        self.assertEqual(t.result, 1)

    def test_exceptions(self):
        def blow_up():
            raise Exception('boom')

        t = redpipe.tasks.AsynchronousTask(target=blow_up)
        t.start()
        self.assertRaises(Exception, lambda: t.result)


class SyncTestCase(unittest.TestCase):
    def test(self):
        def sleeper():
            time.sleep(0.3)
            return 1

        t = redpipe.tasks.SynchronousTask(target=sleeper)
        t.start()
        self.assertEqual(t.result, 1)

    def test_exceptions(self):
        def blow_up():
            raise Exception('boom')

        t = redpipe.tasks.SynchronousTask(target=blow_up)
        t.start()
        self.assertRaises(Exception, lambda: t.result)


class FutureTestCase(unittest.TestCase):
    def test(self):
        f = redpipe.Future()
        self.assertEqual(repr(f), repr(None))
        self.assertRaises(redpipe.ResultNotReady, lambda: str(f))
        self.assertRaises(redpipe.ResultNotReady, lambda: f[:])


class FutureStringTestCase(unittest.TestCase):
    def setUp(self):
        self.result = 'abc'
        self.future = redpipe.Future()
        self.future.set(self.result)

    def test(self):
        self.assertEqual(self.future[0:1], self.result[0:1])
        self.assertEqual(len(self.future), len(self.result))
        self.assertEqual(self.future + 'b', self.result + 'b')
        self.assertEqual(self.future.split(), self.result.split())
        self.assertEqual(repr(self.future), repr(self.result))
        self.assertEqual(str(self.future), str(self.result))
        self.assertEqual(self.future, self.result)
        self.assertEqual(bool(self.future), bool(self.result))


class FutureNoneTestCase(unittest.TestCase):
    def setUp(self):
        self.result = None
        self.future = redpipe.Future()
        self.future.set(self.result)

    def test(self):
        self.assertEqual(repr(self.future), repr(self.result))
        self.assertEqual(str(self.future), str(self.result))
        self.assertEqual(self.future, self.result)
        self.assertEqual(bool(self.future), bool(self.result))
        self.assertTrue(self.future.IS(None))
        self.assertTrue(redpipe.IS(self.future, None))
        self.assertTrue(redpipe.IS(self.future, self.future))
        self.assertTrue(self.future.isinstance(None.__class__))
        self.assertTrue(redpipe.ISINSTANCE(self.future, None.__class__))
        self.assertTrue(redpipe.ISINSTANCE(None, None.__class__))


class FutureIntTestCase(unittest.TestCase):
    def setUp(self):
        self.result = 1
        self.future = redpipe.Future()
        self.future.set(self.result)

    def test(self):
        self.assertEqual(repr(self.future), repr(self.result))
        self.assertEqual(str(self.future), str(self.result))
        self.assertEqual(self.future, self.result)
        self.assertEqual(bool(self.future), bool(self.result))
        self.assertTrue(self.future.IS(self.result))
        self.assertEqual(hash(self.future), hash(self.result))
        self.assertEqual(self.future + 1, self.result + 1)
        self.assertEqual(1 + self.future, 1 + self.result)
        self.assertEqual(self.future - 1, self.result - 1)
        self.assertEqual(1 - self.future, 1 - self.result)
        self.assertTrue(self.future < 2)
        self.assertTrue(self.future <= 2)
        self.assertTrue(self.future > 0)
        self.assertTrue(self.future >= 1)
        self.assertTrue(self.future != 2)
        self.assertEqual(self.future * 1, self.result * 1)
        self.assertEqual(1 * self.future, 1 * self.result)
        self.assertEqual(self.future ** 1, self.result ** 1)
        self.assertEqual(1 ** self.future, 1 ** self.result)
        self.assertEqual(self.future / 1, self.result / 1)
        self.assertEqual(1 / self.future, 1 / self.result)
        self.assertEqual(self.future // 1, self.result // 1)
        self.assertEqual(1 // self.future, 1 // self.result)
        self.assertEqual(self.future % 1, self.result % 1)
        self.assertEqual(1 % self.future, 1 % self.result)
        self.assertEqual(self.future << 1, self.result << 1)
        self.assertEqual(1 << self.future, 1 << self.result)
        self.assertEqual(self.future >> 1, self.result >> 1)
        self.assertEqual(1 >> self.future, 1 >> self.result)
        self.assertEqual(self.future & 1, self.result & 1)
        self.assertEqual(1 & self.future, 1 & self.result)
        self.assertEqual(self.future | 1, self.result | 1)
        self.assertEqual(1 | self.future, 1 | self.result)
        self.assertEqual(self.future ^ 1, self.result ^ 1)
        self.assertEqual(1 ^ self.future, 1 ^ self.result)
        self.assertEqual(bytes(self.future), bytes(self.result))
        self.assertEqual(int(self.future), int(self.result))
        self.assertEqual(float(self.future), float(self.result))
        self.assertEqual(round(self.future), round(self.result))
        self.assertEqual(sum([self.future]), sum([self.result]))


class FutureDictTestCase(unittest.TestCase):
    def setUp(self):
        self.result = {'a': 1, 'b': 2}
        self.future = redpipe.Future()
        self.future.set(self.result)

    def test(self):
        self.assertEqual(self.future.keys(), self.result.keys())
        self.assertEqual(self.future.items(), self.result.items())
        self.assertEqual(self.future, self.result)
        self.assertEqual(dict(self.future), dict(self.result))
        self.assertEqual([k for k in self.future], [k for k in self.result])
        self.assertTrue('a' in self.future)
        self.assertEqual(json.dumps(self.future), json.dumps(self.result))
        self.assertRaises(TypeError, lambda: json.dumps(object()))

        self.assertEqual(self.future.id(), id(self.result))
        self.assertEqual(self.future['a'], self.result['a'])
        self.assertRaises(KeyError, lambda: self.future['xyz'])
        self.assertEqual(pickle.loads(pickle.dumps(self.future)), self.result)


class FutureListTestCase(unittest.TestCase):
    def setUp(self):
        self.result = ['a', 'b', 'c']
        self.future = redpipe.Future()
        self.future.set(self.result)

    def test(self):
        self.assertEqual(self.future, self.result)
        self.assertEqual(list(self.future), list(self.result))
        self.assertEqual([k for k in self.future], [k for k in self.result])
        self.assertTrue('a' in self.future)
        self.assertEqual(json.dumps(self.future), json.dumps(self.result))
        self.assertEqual(self.future.id(), id(self.result))
        self.assertEqual(self.future[1:-1], self.result[1:-1])
        self.assertTrue(self.future.isinstance(self.result.__class__))
        self.assertTrue(self.future.IS(self.result))
        self.assertEqual([i for i in reversed(self.future)],
                         [i for i in reversed(self.result)])


class FutureCallableTestCase(unittest.TestCase):
    def setUp(self):
        def cb():
            return 1

        self.result = cb
        self.future = redpipe.Future()
        self.future.set(self.result)

    def test(self):
        self.assertEqual(self.future, self.result)
        self.assertEqual(self.future(), self.result())
        self.assertEqual(self.future.id(), id(self.result))
        self.assertTrue(self.future.isinstance(self.result.__class__))
        self.assertTrue(self.future.IS(self.result))


class Issue2NamedConnectionsTestCase(unittest.TestCase):
    conn = redislite.StrictRedis()

    class T(redpipe.Struct):
        connection = 't'
        keyspace = 't'

        fields = {
            'foo': redpipe.IntegerField
        }

    class H(redpipe.Hash):
        connection = 't'
        keyspace = 'h'

    def setUp(self):
        redpipe.connect_redis(self.conn, 't')

    def tearDown(self):
        redpipe.reset()

    def test_struct(self):
        with redpipe.pipeline(name='t', autoexec=True) as pipe:
            self.T('1', pipe=pipe, no_op=True).incr('foo', 10)

        with redpipe.pipeline(name=None, autoexec=True) as pipe:
            c = self.T('1', pipe=pipe)
        self.assertEqual(c['foo'], 10)
        self.assertEqual(c['_key'], '1')

    def test_hash(self):
        with redpipe.pipeline(name='t', autoexec=True) as pipe:
            h = self.H(pipe=pipe)
            h.hincrby('foo', 'val', 3)
            res = h.hget('foo', 'val')
        self.assertEqual(res, '3')


class StructExpiresTestCase(unittest.TestCase):
    conn = redislite.StrictRedis()

    class T(redpipe.Struct):
        connection = 't'
        keyspace = 't'
        ttl = 30

        fields = {
            'foo': redpipe.IntegerField
        }

    def setUp(self):
        redpipe.connect_redis(self.conn, 't')

    def tearDown(self):
        redpipe.reset()

    def test_set(self):
        with redpipe.pipeline(name='t', autoexec=True) as pipe:
            t = self.T('1', pipe=pipe, no_op=True)
            t.update({'foo': 1})

            pttl = self.T.core(pipe=pipe).pttl('1')

        self.assertAlmostEqual(pttl, 30000, delta=50)

        with redpipe.pipeline(name=None, autoexec=True) as pipe:
            c = self.T('1', pipe=pipe)
            pttl = self.T.core(pipe=pipe).pttl('1')

        self.assertAlmostEqual(pttl, 30000, delta=100)
        self.assertEqual(c['foo'], 1)
        self.assertEqual(c['_key'], '1')

        with redpipe.pipeline(name=None, autoexec=True) as pipe:
            c = self.T('1', pipe=pipe, no_op=True)
            c.remove(['foo'])
            pttl = self.T.core(pipe=pipe).pttl('1')

        self.assertEqual(pttl, -2)

        with redpipe.pipeline(name=None, autoexec=True) as pipe:
            c = self.T('1', pipe=pipe)
            c.update({'foo': 1, 'bar': 'b'})
            c.remove(['foo'])
            pttl = self.T.core(pipe=pipe).pttl('1')

        self.assertEqual(dict(c), {'_key': '1', 'bar': 'b'})
        self.assertAlmostEqual(pttl, 30000, delta=50)

    def test_incr(self):
        with redpipe.pipeline(name='t', autoexec=True) as pipe:
            self.T('1', pipe=pipe, no_op=True).incr('foo', 10)
            pttl = self.T.core(pipe=pipe).pttl('1')

        self.assertAlmostEqual(pttl, 30000, delta=50)

        with redpipe.pipeline(name=None, autoexec=True) as pipe:
            c = self.T('1', pipe=pipe)
            pttl = self.T.core(pipe=pipe).pttl('1')

        self.assertAlmostEqual(pttl, 30000, delta=100)
        self.assertEqual(c['foo'], 10)
        self.assertEqual(c['_key'], '1')


class HashedStringTestCase(BaseTestCase):
    class Data(redpipe.HashedString):
        keyspace = 'my_index'
        shard_count = 3

    def test(self):
        with redpipe.pipeline(autoexec=True) as pipe:
            set_res = self.Data(pipe).set('a', 'foo')
            get_res = self.Data(pipe).get('a')
            setnx_res = self.Data(pipe).setnx('a', 'boo')
            mget_res = self.Data(pipe).mget(['a'])
            strlen_res = self.Data(pipe).strlen('a')
            self.Data(pipe)['a'] = 'boo'
            dict_res = self.Data(pipe)['a']

        self.assertEqual(set_res, 1)
        self.assertEqual(get_res, 'foo')
        self.assertEqual(setnx_res, 0)
        self.assertEqual(['foo'], mget_res)
        self.assertEqual(3, strlen_res)
        self.assertEqual('boo', dict_res)

        data = {k: v for k, v in self.Data().scan_iter()}
        self.assertEqual(data, {'a': 'boo'})

        with redpipe.pipeline(autoexec=True) as pipe:
            remove_res = self.Data(pipe).delete('a')
            get_res = self.Data(pipe).get('a')

        self.assertEqual(1, remove_res)
        self.assertEqual(None, get_res)
        self.assertEqual(b'my_index{1}',
                         self.Data.core().redis_key(self.Data.shard('a')))

    def test_incr(self):
        with redpipe.pipeline(autoexec=True) as pipe:
            incr_res = self.Data(pipe).incr('b')
            incrby_res = self.Data(pipe).incrby('b', '1')
            incrbyfloat_res = self.Data(pipe).incrbyfloat('b', 0.2)

        self.assertEqual(1, incr_res)
        self.assertEqual(2, incrby_res)
        self.assertEqual(2.2, incrbyfloat_res)


if __name__ == '__main__':
    try:
        unittest.main(verbosity=2, warnings='ignore')
    except TypeError:
        unittest.main(verbosity=2)
