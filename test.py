#!/usr/bin/env python
import json
import unittest
import redis
import redislite
import redpipe
import mock
import redpipe.tasks
import time


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

        self.assertEqual(g.result, b'bar')

    def test_zset(self):
        p = redpipe.pipeline()

        p.zadd('foo', 1, 'a')
        p.zadd('foo', 2, 'b')
        p.zadd('foo', 3, 'c')
        z = p.zrange('foo', 0, -1)

        # can't access it until it's ready
        self.assertRaises(redpipe.ResultNotReady, lambda: z.result)
        p.execute()

        self.assertEqual(z.result, [b'a', b'b', b'c'])

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
        self.assertEqual(ref.result, 1)
        self.assertEqual(self.r.zrange('foo', 0, -1), [b'a'])

        p = redpipe.pipeline()
        ref = p.zadd('foo', 1, 'a')
        p.reset()
        p.execute()
        self.assertRaises(redpipe.ResultNotReady, lambda: ref.result)


class StringCollectionTestCase(BaseTestCase):
    def test(self):
        class Foo(redpipe.String):
            namespace = 'F'

        with redpipe.pipeline(autocommit=True) as pipe:
            f = Foo('1', pipe=pipe)
            set_ref = f.set('bar')
            get_ref = f.get()

        self.assertEqual(set_ref.result, 1)
        self.assertEqual(get_ref.result, 'bar')


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


class ModelTestCase(BaseTestCase):
    class User(redpipe.Model):
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
        class Multi(redpipe.Model):
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

    def disabled_async_test(self):
        with mock.patch('redpipe.async.Task',
                        redpipe.tasks.AsynchronousTask):
            self.test_single_nested()
            self.tearDown()
            self.test_pipeline_nested_mismatched_name()
            self.tearDown()
            self.test_multi_invalid_connection()
            self.tearDown()
            self.test_sleeping_cb()

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


class StringTestCase(BaseTestCase):
    class Flag(redpipe.String):
        _keyspace = 'STRING'

    def test(self):
        with redpipe.pipeline(autocommit=True) as pipe:
            f = self.Flag('1', pipe=pipe)
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

        with redpipe.pipeline(autocommit=True) as pipe:
            f = self.Flag('2', pipe=pipe)
            restore = f.restore(serialize.result)
            ref = f.get()
            idle = f.object('IDLETIME')
            persist = f.persist()
            incr = f.incr()
            incrby = f.incrby(2)
            incrbyfloat = f.incrbyfloat(2.1)
            setnx = f.setnx('foo')
            getaftersetnx = f.get()
        self.assertEqual(restore.result, 1)
        self.assertEqual(ref.result, '2')
        self.assertEqual(str(f), '<Flag:2>')
        self.assertEqual(idle.result, 0)
        self.assertEqual(persist.result, 0)
        self.assertEqual(incr.result, 3)
        self.assertEqual(incrby.result, 5)
        self.assertEqual(incrbyfloat.result, 7.1)
        self.assertEqual(setnx.result, 0)
        self.assertEqual(getaftersetnx.result, '7.1')


class SetTestCase(BaseTestCase):
    class Col(redpipe.Set):
        _keyspace = 'SET'

    def test(self):
        with redpipe.pipeline(autocommit=True) as pipe:
            c = self.Col('1', pipe=pipe)
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


class ListTestCase(BaseTestCase):
    class Col(redpipe.List):
        _keyspace = 'LIST'

    def test(self):
        with redpipe.pipeline(autocommit=True) as pipe:
            c = self.Col('1', pipe=pipe)
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


class SortedSetTestCase(BaseTestCase):
    class Collection(redpipe.SortedSet):
        _keyspace = 'SORTEDSET'

    def test(self):
        with redpipe.pipeline(autocommit=True) as pipe:
            c = self.Collection('1', pipe=pipe)
            c.add('2', 2)
            c.add('3', 3)
            add = c.add('4', 4)
            zaddincr = c.zadd('4', 1, incr=True)
            zscore_after_incr = c.zscore('4')
            zaddnx = c.zadd('4', 4.1, nx=True)
            zaddxx = c.zadd('4', 4.2, xx=True)
            zaddch = c.zadd('4', 4.3, ch=True)
            zscore = c.zscore('4')
            remove = c.remove('4')
            members = c.members()
            zaddmulti = c.zadd({'4': 4, '5': 5})
            zincrby = c.zincrby('5', 2)
            zrevrank = c.zrevrank('5')
            zrevrange = c.zrevrange(0, 1)
            zrange_withscores = c.zrange(0, 1, withscores=True)
            zrevrange_withscores = c.zrevrange(0, 1, withscores=True)

            self.assertRaises(
                redpipe.InvalidOperation,
                lambda: c.zadd('4', 4, xx=True, nx=True))
            c.delete()
            zrange = c.zrange(0, -1)
            self.assertRaises(redpipe.ResultNotReady, lambda: members.result)
        self.assertEqual(add.result, 1)
        self.assertEqual(zaddincr.result, 5)
        self.assertEqual(zscore_after_incr.result, 5)
        self.assertEqual(zaddnx.result, 0)
        self.assertEqual(zaddxx.result, 0)
        self.assertEqual(zaddch.result, 1)
        self.assertEqual(zscore.result, 4.3)
        self.assertEqual(remove.result, 1)
        self.assertEqual(members.result, ['2', '3'])
        self.assertEqual(zaddmulti.result, 2)
        self.assertEqual(zrange.result, [])
        self.assertEqual(zincrby.result, 7.0)
        self.assertEqual(zrevrank.result, 0)
        self.assertEqual(zrevrange.result, ['5', '4'])
        self.assertEqual(zrange_withscores.result, [['2', 2.0], ['3', 3.0]])
        self.assertEqual(zrevrange_withscores.result, [['5', 7.0], ['4', 4.0]])

        with redpipe.pipeline(autocommit=True) as pipe:
            c = self.Collection('1', pipe=pipe)
            c.zadd('a', 1)
            c.zadd('b', 2)
            zrangebyscore = c.zrangebyscore(0, 10, start=0, num=1)
            zrangebyscore_withscores = c.zrangebyscore(
                0, 10, start=0, num=1, withscores=True)
            zrevrangebyscore = c.zrevrangebyscore(10, 0, start=0, num=1)
            zrevrangebyscore_withscores = c.zrevrangebyscore(
                10, 0, start=0, num=1, withscores=True)
            zcard = c.zcard()
            zrank = c.zrank('b')
            zremrangebyrank = c.zremrangebyrank(0, 0)
            zremrangebyscore = c.zremrangebyscore(2, 2)

        self.assertEqual(zrangebyscore.result, ['a'])
        self.assertEqual(zrangebyscore_withscores.result, [['a', 1.0]])
        self.assertEqual(zrevrangebyscore.result, ['b'])
        self.assertEqual(zrevrangebyscore_withscores.result, [['b', 2.0]])
        self.assertEqual(zcard.result, 2)
        self.assertEqual(zrank.result, 1)
        self.assertEqual(zremrangebyrank.result, 1)
        self.assertEqual(zremrangebyscore.result, 1)


class HashTestCase(BaseTestCase):
    class Collection(redpipe.Hash):
        _keyspace = 'HASH'

    def test(self):
        with redpipe.pipeline(autocommit=True) as pipe:
            c = self.Collection('1', pipe=pipe)
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


class AsyncTestCase(unittest.TestCase):

    def disabled_test(self):
        def sleeper():
            time.sleep(0.3)
            return 1

        t = redpipe.tasks.AsynchronousTask(target=sleeper)
        t.start()
        self.assertEqual(t.result, 1)

if __name__ == '__main__':
    unittest.main(verbosity=2)
