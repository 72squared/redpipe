#!/usr/bin/env python
import json
import unittest
import time
import redis
import redislite
import rediscluster
import rediscluster.exceptions
import redpipe
import redpipe.tasks


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
        field = redpipe.IntegerField
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
        field = redpipe.TextField
        self.assertFalse(field.validate(1))
        self.assertFalse(field.validate(False))
        self.assertFalse(field.validate(0.12456))
        self.assertTrue(field.validate('dddd'))
        self.assertTrue(field.validate(json.loads('"15\u00f8C"')))
        self.assertTrue(field.validate(''))
        self.assertTrue(field.validate('a'))
        self.assertTrue(field.validate('1'))
        self.assertEqual(field.to_persistence('1'), '1')
        self.assertEqual(field.to_persistence('1.2'), '1.2')
        self.assertEqual(field.to_persistence('abc123$!'), 'abc123$!')
        sample = json.loads('"15\u00f8C"')
        self.assertEqual(
            field.from_persistence(field.to_persistence(sample)),
            sample
        )

    def test_ascii(self):
        field = redpipe.AsciiField
        self.assertFalse(field.validate(1))
        self.assertFalse(field.validate(False))
        self.assertFalse(field.validate(0.12456))
        self.assertTrue(field.validate('dddd'))
        self.assertFalse(field.validate(json.loads('"15\u00f8C"')))
        self.assertTrue(field.validate(''))
        self.assertTrue(field.validate('a'))
        self.assertTrue(field.validate('1'))
        self.assertEqual(field.to_persistence('1'), '1')
        self.assertEqual(field.to_persistence('1.2'), '1.2')
        self.assertEqual(field.to_persistence('abc123$!'), 'abc123$!')
        sample = '#$%^&*()!@#aABc'
        self.assertEqual(
            field.from_persistence(field.to_persistence(sample)),
            sample
        )

    def test_json(self):
        field = redpipe.JsonField
        self.assertFalse(field.validate(1))
        self.assertFalse(field.validate(False))
        self.assertFalse(field.validate(0.12456))
        self.assertFalse(field.validate('dddd'))
        self.assertTrue(field.validate([1]))
        self.assertTrue(field.validate({'a': 1}))
        data = {'a': 1}
        self.assertEqual(
            field.from_persistence(field.to_persistence(data)),
            data)

        data = ['a', 1]
        self.assertEqual(
            field.from_persistence(field.to_persistence(data)),
            data)

        self.assertEqual(field.from_persistence(data), data)

    def test_list(self):
        field = redpipe.ListField
        self.assertFalse(field.validate(1))
        self.assertFalse(field.validate(False))
        self.assertFalse(field.validate(0.12456))
        self.assertFalse(field.validate('dddd'))
        self.assertTrue(field.validate([1]))
        self.assertFalse(field.validate({'a': 1}))
        data = ['a', 1]
        self.assertEqual(
            field.from_persistence(field.to_persistence(data)),
            data)

        self.assertEqual(field.from_persistence(data), data)

    def test_dict(self):
        field = redpipe.DictField
        self.assertFalse(field.validate(1))
        self.assertFalse(field.validate(False))
        self.assertFalse(field.validate(0.12456))
        self.assertFalse(field.validate('dddd'))
        self.assertFalse(field.validate([1]))
        self.assertTrue(field.validate({'a': 1}))
        data = {'a': 1}
        self.assertEqual(
            field.from_persistence(field.to_persistence(data)),
            data)

    def test_string_list(self):
        field = redpipe.StringListField
        self.assertFalse(field.validate(1))
        self.assertFalse(field.validate(False))
        self.assertFalse(field.validate(0.12456))
        self.assertFalse(field.validate('dddd'))
        self.assertTrue(field.validate(['1']))
        self.assertTrue(field.validate(None))
        self.assertFalse(field.validate([1]))
        self.assertFalse(field.validate({'a': 1}))
        data = ['a', 'b', 'c']
        self.assertEqual(
            field.from_persistence(field.to_persistence(data)),
            data)

        self.assertEqual(field.from_persistence(data), data)
        self.assertIsNone(field.from_persistence(''))


class StructTestCase(BaseTestCase):
    class User(redpipe.Struct):
        _keyspace = 'U'
        _fields = {
            'first_name': redpipe.TextField,
            'last_name': redpipe.TextField,
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
        u.remove(['last_name', 'test_field'])
        self.assertEqual(u.last_name, None)
        self.assertRaises(AttributeError, lambda: u.non_existent_field)
        u.change(first_name='Wilma')
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

    def test_core(self):
        self.create_user('1')
        ref = self.User.core('1').hgetall()
        self.assertEqual(ref.result['first_name'], 'first1')

    def test_pipeline(self):
        user_ids = ["%s" % i for i in range(1, 3)]
        with redpipe.pipeline(autocommit=True) as pipe:
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
        class Multi(redpipe.Struct):
            _keyspace = 'M'
            _fields = {
                'boolean': redpipe.BooleanField,
                'integer': redpipe.IntegerField,
                'float': redpipe.FloatField,
                'text': redpipe.TextField,
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
            redpipe.InvalidFieldValue,
            lambda: Multi('m3', text=123))

        self.assertRaises(
            redpipe.InvalidFieldValue,
            lambda: Multi('m3', boolean='abc'))

    def test_extra_fields(self):
        u = self.User('1', first_name='Bob', last_name='smith',
                      nickname='BUBBA')
        u = self.User('1')
        self.assertEqual(u._key, '1')
        self.assertEqual(u['_key'], '1')
        self.assertEqual(u.nickname, 'BUBBA')
        self.assertEqual(u.get('nickname'), 'BUBBA')
        self.assertEqual(u.get('nonexistent', 'test'), 'test')
        self.assertRaises(AttributeError, lambda: u.nonexistent)
        self.assertRaises(KeyError, lambda: u['nonexistent'])

    def test_missing_fields(self):
        u = self.User('1', first_name='Bob')
        u = self.User('1')
        self.assertEqual(u.last_name, None)


class ConnectTestCase(unittest.TestCase):
    def tearDown(self):
        redpipe.reset()

    def incr_a(self, key, pipe=None):
        with redpipe.pipeline(pipe, name='a', autocommit=True) as pipe:
            return pipe.incr(key)

    def incr_b(self, key, pipe=None):
        with redpipe.pipeline(pipe, name='b', autocommit=True) as pipe:
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
        redpipe.connect(r.pipeline)

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
            with redpipe.pipeline(pipe, name='a', autocommit=1) as pipe:
                return self.incr_a('foo', pipe=pipe)

        def top_level(pipe=None):
            with redpipe.pipeline(pipe, name='a', autocommit=1) as pipe:
                return mid_level(pipe)

        with redpipe.pipeline(name='a', autocommit=1) as pipe:
            ref = top_level(pipe)
            self.assertRaises(redpipe.ResultNotReady, lambda: ref.result)

        self.assertEqual(ref.result, 1)

    def test_async(self):
        try:
            redpipe.enable_threads()
            self.test_single_nested()
            self.tearDown()
            self.test_pipeline_nested_mismatched_name()
            self.tearDown()
            self.test_multi_invalid_connection()
            self.tearDown()
            self.test_sleeping_cb()
        finally:
            redpipe.disable_threads()

    def test_sleeping_cb(self):
        redpipe.connect_redis(redislite.Redis(), 'a')
        redpipe.connect_redis(redislite.Redis(), 'b')

        with redpipe.pipeline(autocommit=True, name='a') as pipe:
            pipe.set('foo', '1')
            with redpipe.pipeline(pipe=pipe, name='b', autocommit=True) as p:
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
            with redpipe.pipeline(autocommit=True) as pipe:
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
        with redpipe.pipeline(autocommit=True) as pipe:
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
            with redpipe.pipeline(autocommit=1) as pipe:
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
        redpipe.connect_rediscluster(r, 'test')
        with redpipe.pipeline(name='test') as pipe:
            pipe.set('foo', 'bar')
            self.assertRaises(Exception, pipe.execute)


class StringTestCase(BaseTestCase):
    class Data(redpipe.String):
        _keyspace = 'STRING'

    def test(self):
        with redpipe.pipeline(autocommit=True) as pipe:
            s = self.Data('1', pipe=pipe)
            self.assertEqual(s.redis_key, b'STRING{1}')
            s.set('2')
            before = s.get()
            serialize = s.dump()
            s.expire(3)
            ttl = s.ttl()
            s.delete()
            exists = s.exists()
            after = s.get()
            self.assertRaises(redpipe.ResultNotReady, lambda: before.result)

        self.assertEqual(before.result, '2')
        self.assertEqual(after.result, None)
        self.assertAlmostEqual(ttl, 3, delta=1)
        self.assertIsNotNone(serialize.result)
        self.assertFalse(exists.result)

        with redpipe.pipeline(autocommit=True) as pipe:
            s = self.Data('2', pipe=pipe)
            restore = s.restore(serialize.result)
            ref = s.get()
            idle = s.object('IDLETIME')
            persist = s.persist()
            incr = s.incr()
            incrby = s.incrby(2)
            incrbyfloat = s.incrbyfloat(2.1)
            setnx = s.setnx('foo')
            getaftersetnx = s.get()
            setex = s.setex('bar', 60)
            getaftersetex = s.get()
            ttl = s.ttl()
        self.assertEqual(restore.result, 1)
        self.assertEqual(ref.result, '2')
        self.assertEqual(str(s), '<Data:2>')
        self.assertEqual(idle.result, 0)
        self.assertEqual(persist.result, 0)
        self.assertEqual(incr.result, 3)
        self.assertEqual(incrby.result, 5)
        self.assertEqual(incrbyfloat.result, 7.1)
        self.assertEqual(setnx.result, 0)
        self.assertEqual(getaftersetnx.result, '7.1')
        self.assertEqual(setex, 1)
        self.assertEqual(getaftersetex, 'bar')
        self.assertAlmostEqual(ttl, 60, delta=1)

        with redpipe.pipeline(autocommit=True) as pipe:
            s = self.Data('3', pipe=pipe)
            s.set('bar')
            append = s.append('r')
            substr = s.substr(1, 3)
            strlen = s.strlen()
            setrange = s.setrange(1, 'azz')
            get = s.get()

        self.assertEqual(append, 4)
        self.assertEqual(strlen, 4)
        self.assertEqual(substr, 'arr')
        self.assertEqual(setrange, 4)
        self.assertEqual(get, 'bazz')

    def test_bitwise(self):
        with redpipe.pipeline(autocommit=True) as pipe:
            s = self.Data('1', pipe=pipe)
            setbit = s.setbit(2, 1)
            getbit = s.getbit(2)
            bitcount = s.bitcount()

        self.assertEqual(setbit, 0)
        self.assertEqual(getbit, 1)
        self.assertEqual(bitcount, 1)

    def test_bare(self):
        with redpipe.pipeline(autocommit=True) as pipe:
            f = redpipe.String('foo', pipe=pipe)
            self.assertEqual(f.redis_key, b'foo')
            f.set('2')
            before = f.get()
            serialize = f.dump()
            f.expire(3)
            ttl = f.ttl()

            f.delete()
            exists = f.exists()
            after = f.get()
            self.assertRaises(redpipe.ResultNotReady, lambda: before.result)
        self.assertEqual(before.result, '2')
        self.assertEqual(after.result, None)
        self.assertAlmostEqual(ttl.result, 3, delta=1)
        self.assertIsNotNone(serialize.result)
        self.assertFalse(exists.result)


class SetTestCase(BaseTestCase):
    class Data(redpipe.Set):
        _keyspace = 'SET'

    def test(self):
        with redpipe.pipeline(autocommit=True) as pipe:
            c = self.Data('1', pipe=pipe)
            sadd = c.sadd(['a', 'b', 'c'])
            saddnx = c.sadd('a')
            srem = c.srem('c')
            smembers = c.smembers()
            card = c.scard()
            ismember_a = c.sismember('a')
            c.srem('b')
            ismember_b = c.sismember('b')
            srandmember = c.srandmember()
            spop = c.spop()

        self.assertEqual(sadd.result, 3)
        self.assertEqual(saddnx.result, 0)
        self.assertEqual(srem.result, 1)
        self.assertEqual(smembers.result, {'a', 'b'})
        self.assertIn(spop.result, {'a', 'b'})
        self.assertEqual(card.result, 2)
        self.assertTrue(ismember_a.result)
        self.assertFalse(ismember_b.result)
        self.assertTrue(srandmember.result, b'a')

    def test_scan(self):
        with redpipe.pipeline(autocommit=True) as pipe:
            s = self.Data('1', pipe=pipe)
            s.sadd('a1', 'a2', 'b1', 'b2')
            sscan = s.sscan(0, match='a*')

        self.assertEqual(sscan[0], 0)
        self.assertEqual(set(sscan[1]), {'a1', 'a2'})

        with redpipe.pipeline(autocommit=True) as pipe:
            self.assertRaises(
                redpipe.InvalidOperation,
                lambda: {k for k in self.Data('1', pipe=pipe).sscan_iter()})

        data = {k for k in self.Data('1').sscan_iter()}
        self.assertEqual(data, {'a1', 'a2', 'b1', 'b2'})


class ListTestCase(BaseTestCase):
    class Data(redpipe.List):
        _keyspace = 'LIST'

    def test(self):
        with redpipe.pipeline(autocommit=True) as pipe:
            c = self.Data('1', pipe=pipe)
            lpush = c.lpush('a', 'b', 'c', 'd')
            members = c.members()
            rpush = c.rpush('e')
            llen = c.llen()
            lrange = c.lrange(0, -1)
            rpop = c.rpop()
            lrem = c.lrem('a', 1)
            ltrim = c.ltrim(0, 1)
            members_after_ltrim = c.members()
            lindex = c.lindex(1)
            lset = c.lset(1, 'a')
            lindex_after = c.lindex(1)
            lpop = c.lpop()

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
        with redpipe.pipeline(autocommit=True) as pipe:
            self.Data('1a', pipe=pipe).lpush('1')
            self.Data('1b', pipe=pipe).lpush('1')
            self.Data('2a', pipe=pipe).lpush('1')
            self.Data('2b', pipe=pipe).lpush('1')
            sscan = self.Data.scan(0, match='1*', pipe=pipe)
            sscan_all = self.Data.scan(pipe=pipe)

        self.assertEqual(sscan[0], 0)
        self.assertEqual(set(sscan[1]), {'1a', '1b'})
        self.assertEqual(set(sscan_all[1]), {'1a', '1b', '2a', '2b'})
        self.assertEqual({k for k in self.Data.scan_iter()},
                         {'1a', '1b', '2a', '2b'})

    def test_scan_with_no_keyspace(self):
        with redpipe.pipeline(autocommit=True) as pipe:
            redpipe.List('1a', pipe=pipe).lpush('1')
            redpipe.List('1b', pipe=pipe).lpush('1')
            redpipe.List('2a', pipe=pipe).lpush('1')
            redpipe.List('2b', pipe=pipe).lpush('1')
            sscan = redpipe.List.scan(0, match='1*', pipe=pipe)

        self.assertEqual(sscan[0], 0)
        self.assertEqual(set(sscan[1]), {'1a', '1b'})


class SortedSetTestCase(BaseTestCase):
    class Data(redpipe.SortedSet):
        _keyspace = 'SORTEDSET'

    def test(self):
        with redpipe.pipeline(autocommit=True) as pipe:
            s = self.Data('1', pipe=pipe)
            s.add('2', 2)
            s.add('3', 3)
            add = s.add('4', 4)
            zaddincr = s.zadd('4', 1, incr=True)
            zscore_after_incr = s.zscore('4')
            zaddnx = s.zadd('4', 4.1, nx=True)
            zaddxx = s.zadd('4', 4.2, xx=True)
            zaddch = s.zadd('4', 4.3, ch=True)
            zscore = s.zscore('4')
            remove = s.remove('4')
            members = s.members()
            zaddmulti = s.zadd({'4': 4, '5': 5})
            zincrby = s.zincrby('5', 2)
            zrevrank = s.zrevrank('5')
            zrevrange = s.zrevrange(0, 1)
            zrange_withscores = s.zrange(0, 1, withscores=True)
            zrevrange_withscores = s.zrevrange(0, 1, withscores=True)

            self.assertRaises(
                redpipe.InvalidOperation,
                lambda: s.zadd('4', 4, xx=True, nx=True))
            s.delete()
            zrange = s.zrange(0, -1)
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
        self.assertEqual(zrange_withscores, [['2', 2.0], ['3', 3.0]])
        self.assertEqual(zrevrange_withscores, [['5', 7.0], ['4', 4.0]])

        with redpipe.pipeline(autocommit=True) as pipe:
            s = self.Data('1', pipe=pipe)
            s.zadd('a', 1)
            s.zadd('b', 2)
            zrangebyscore = s.zrangebyscore(0, 10, start=0, num=1)
            zrangebyscore_withscores = s.zrangebyscore(
                0, 10, start=0, num=1, withscores=True)
            zrevrangebyscore = s.zrevrangebyscore(10, 0, start=0, num=1)
            zrevrangebyscore_withscores = s.zrevrangebyscore(
                10, 0, start=0, num=1, withscores=True)
            zcard = s.zcard()
            zrank = s.zrank('b')
            zlexcount = s.zlexcount('-', '+')
            zrangebylex = s.zrangebylex('-', '+')
            zrevrangebylex = s.zrevrangebylex('+', '-')
            zremrangebyrank = s.zremrangebyrank(0, 0)
            zremrangebyscore = s.zremrangebyscore(2, 2)
            zremrangebylex = s.zremrangebylex('-', '+')

        self.assertEqual(zrangebyscore, ['a'])
        self.assertEqual(zrangebyscore_withscores, [['a', 1.0]])
        self.assertEqual(zrevrangebyscore, ['b'])
        self.assertEqual(zrevrangebyscore_withscores, [['b', 2.0]])
        self.assertEqual(zcard, 2)
        self.assertEqual(zrank, 1)
        self.assertEqual(zremrangebyrank, 1)
        self.assertEqual(zremrangebyscore, 1)
        self.assertEqual(zlexcount, 2)
        self.assertEqual(zrangebylex, ['a', 'b'])
        self.assertEqual(zrevrangebylex, ['b', 'a'])
        self.assertEqual(zremrangebylex, 0)

    def test_scan(self):
        with redpipe.pipeline(autocommit=True) as pipe:
            s = self.Data('1', pipe=pipe)
            s.zadd('a1', 1)
            s.zadd('a2', 2)
            s.zadd('b1', 1)
            s.zadd('b2', 2)
            sscan = s.zscan(0, match='a*')

        self.assertEqual(sscan[0], 0)
        self.assertEqual(set(sscan[1]), {('a1', 1.0), ('a2', 2.0)})

        with redpipe.pipeline(autocommit=True) as pipe:
            self.assertRaises(
                redpipe.InvalidOperation,
                lambda: {k for k in self.Data('1', pipe=pipe).zscan_iter()})

        data = {k for k in self.Data('1').zscan_iter()}
        expected = {('a1', 1.0), ('a2', 2.0), ('b1', 1.0), ('b2', 2.0)}
        self.assertEqual(data, expected)


class HashTestCase(BaseTestCase):
    class Data(redpipe.Hash):
        _keyspace = 'HASH'

    def test(self):
        with redpipe.pipeline(autocommit=True) as pipe:
            c = self.Data('1', pipe=pipe)
            hset = c.hset('a', '1')
            hmset = c.hmset({'b': '2', 'c': '3', 'd': '4'})
            hsetnx = c.hsetnx('b', '9999')
            hget = c.hget('a')
            hgetall = c.hgetall()
            hlen = c.hlen()
            hdel = c.hdel('a', 'b')
            hkeys = c.hkeys()
            hexists = c.hexists('c')
            hincrby = c.hincrby('d', 2)
            hmget = c.hmget(['c', 'd'])
            hvals = c.hvals()

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
        with redpipe.pipeline(autocommit=True) as pipe:
            s = self.Data('1', pipe=pipe)
            s.hmset({'a1': '1', 'a2': '2', 'b1': '1', 'b2': '2'})
            hscan = s.hscan(0, match='a*')

        self.assertEqual(hscan[0], 0)
        self.assertEqual(hscan[1], {'a1': '1', 'a2': '2'})

        with redpipe.pipeline(autocommit=True) as pipe:
            s = self.Data('1', pipe=pipe)
            self.assertRaises(redpipe.InvalidOperation,
                              lambda: [v for v in s.hscan_iter()])

        data = {k: v for k, v in self.Data('1').hscan_iter()}
        self.assertEqual(data, {'b2': '2', 'b1': '1', 'a1': '1', 'a2': '2'})


class HashFieldsTestCase(BaseTestCase):
    class Data(redpipe.Hash):
        _keyspace = 'HASH'
        _fields = {
            'b': redpipe.BooleanField,
            'i': redpipe.IntegerField,
            'f': redpipe.FloatField,
            't': redpipe.TextField,
            'j': redpipe.JsonField,
            'l': redpipe.ListField,
            'd': redpipe.DictField,
            'sl': redpipe.StringListField,
        }

    def test(self):
        with redpipe.pipeline(autocommit=True) as pipe:
            c = self.Data('1', pipe=pipe)
            hset = c.hset('i', 1)
            hmset = c.hmset({'b': True, 'f': 3.1, 't': 'a'})
            hsetnx = c.hsetnx('b', False)
            hget = c.hget('b')
            hgetall = c.hgetall()
            hincrby = c.hincrby('i', 2)
            hincrbyfloat = c.hincrbyfloat('f', 2.1)
            hmget = c.hmget(['f', 'b'])

        self.assertEqual(hset.result, True)
        self.assertEqual(hmset.result, True)
        self.assertEqual(hsetnx.result, 0)
        self.assertEqual(hget.result, True)
        self.assertEqual(
            hgetall.result,
            {'b': True, 'i': 1, 'f': 3.1, 't': 'a'})
        self.assertEqual(hincrby.result, 3)
        self.assertEqual(hincrbyfloat.result, 5.2)
        self.assertEqual(hmget.result, [5.2, True])

    def test_invalid_value(self):
        with redpipe.pipeline() as pipe:
            c = self.Data('1', pipe=pipe)
            self.assertRaises(
                redpipe.InvalidFieldValue, lambda: c.hset('i', 'a'))
            self.assertRaises(
                redpipe.InvalidFieldValue, lambda: c.hset('b', '1'))
            self.assertRaises(
                redpipe.InvalidFieldValue, lambda: c.hset('t', 1))

            c.hset('f', 1)

            self.assertRaises(
                redpipe.InvalidFieldValue, lambda: c.hset('f', '1'))

    def test_json_list(self):
        with redpipe.pipeline(autocommit=True) as pipe:
            data = [1, 'a']
            c = self.Data('1', pipe=pipe)
            hset = c.hset('j', data)
            hget = c.hget('j')
            hmget = c.hmget(['j'])
            hgetall = c.hgetall()

        self.assertEqual(hset, 1)
        self.assertEqual(hget, data)
        self.assertEqual(hmget, [data])
        self.assertEqual(hgetall, {'j': data})

    def test_json_dict(self):
        with redpipe.pipeline(autocommit=True) as pipe:
            data = {'a': 1, 'b': 'test'}
            c = self.Data('1', pipe=pipe)
            hset = c.hset('j', data)
            hget = c.hget('j')
            hmget = c.hmget(['j'])
            hgetall = c.hgetall()

        self.assertEqual(hset, 1)
        self.assertEqual(hget, data)
        self.assertEqual(hmget, [data])
        self.assertEqual(hgetall, {'j': data})

    def test_dict(self):
        with redpipe.pipeline(autocommit=True) as pipe:
            data = {'a': 1, 'b': 'test'}
            c = self.Data('d', pipe=pipe)
            hset = c.hset('d', data)
            hget = c.hget('d')
            hmget = c.hmget(['d'])
            hgetall = c.hgetall()

        self.assertEqual(hset, 1)
        self.assertEqual(hget, data)
        self.assertEqual(hmget, [data])
        self.assertEqual(hgetall, {'d': data})

    def test_list(self):
        with redpipe.pipeline(autocommit=True) as pipe:
            data = [1, 'a']
            c = self.Data('1', pipe=pipe)
            hset = c.hset('l', data)
            hget = c.hget('l')
            hmget = c.hmget(['l'])
            hgetall = c.hgetall()

        self.assertEqual(hset, 1)
        self.assertEqual(hget, data)
        self.assertEqual(hmget, [data])
        self.assertEqual(hgetall, {'l': data})

    def test_string_list(self):
        with redpipe.pipeline(autocommit=True) as pipe:
            data = ['a', 'b']
            c = self.Data('1', pipe=pipe)
            hget_pre = c.hget('sl')
            hset = c.hset('sl', data)
            hget = c.hget('sl')
            hmget = c.hmget(['sl'])
            hgetall = c.hgetall()

        self.assertEqual(hget_pre, None)
        self.assertEqual(hset, 1)
        self.assertEqual(hget, data)
        self.assertEqual(hmget, [data])
        self.assertEqual(hgetall, {'sl': data})


class HyperloglogTestCase(BaseTestCase):
    class Data(redpipe.HyperLogLog):
        _keyspace = 'HYPERLOGLOG'

    def test(self):
        with redpipe.pipeline(autocommit=True) as pipe:
            c = self.Data('1', pipe=pipe)
            pfadd = c.pfadd('a', 'b', 'c')
            pfcount = c.pfcount()

        self.assertEqual(pfadd, 1)
        self.assertEqual(pfcount, 3)


class AsyncTestCase(unittest.TestCase):
    def test(self):
        def sleeper():
            time.sleep(0.3)
            return 1

        t = redpipe.tasks.AsynchronousTask(target=sleeper)
        t.start()
        self.assertEqual(t.result, 1)


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
        self.assertTrue(self.future.isinstance(None.__class__))


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
        self.assertEqual(self.future.json, json.dumps(self.result))
        self.assertEqual(self.future.id(), id(self.result))
        self.assertEqual(self.future['a'], self.result['a'])
        self.assertRaises(KeyError, lambda: self.future['xyz'])


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
        self.assertEqual(self.future.json, json.dumps(self.result))
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


if __name__ == '__main__':
    unittest.main(verbosity=2)
