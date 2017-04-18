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
        _namespace = 'U'
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
            _namespace = 'M'
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


class StringTestCase(BaseTestCase):
    class Flag(rediswrap.String):
        _namespace = 'STRING'

    def test(self):
        with rediswrap.PipelineContext() as pipe:
            f = self.Flag('1', pipe=pipe)
            f.set('2')
            before = f.get()
            serialize = f.dump()
            f.expire(3)
            ttl = f.ttl()

            f.delete()
            exists = f.exists()
            after = f.get()
            self.assertRaises(AttributeError, lambda: before.result)
        self.assertEqual(before.result, b'2')
        self.assertEqual(after.result, None)
        self.assertAlmostEqual(ttl.result, 3, delta=1)
        self.assertIsNotNone(serialize.result)
        self.assertFalse(exists.result)

        with rediswrap.PipelineContext() as pipe:
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
        self.assertEqual(ref.result, b'2')
        self.assertEqual(str(f), '<Flag:2>')
        self.assertEqual(idle.result, 0)
        self.assertEqual(persist.result, 0)
        self.assertEqual(incr.result, 3)
        self.assertEqual(incrby.result, 5)
        self.assertEqual(incrbyfloat.result, 7.1)
        self.assertEqual(setnx.result, 0)
        self.assertEqual(getaftersetnx.result, b'7.1')


class SetTestCase(BaseTestCase):
    class Col(rediswrap.Set):
        _namespace = 'SET'

    def test(self):
        with rediswrap.PipelineContext() as pipe:
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
        self.assertEqual(smembers.result, {b'a', b'b'})
        self.assertIn(spop.result, {b'a', b'b'})
        self.assertEqual(card.result, 2)
        self.assertTrue(ismember_a.result)
        self.assertFalse(ismember_b.result)
        self.assertTrue(srandmember.result, b'a')


class ListTestCase(BaseTestCase):
    class Col(rediswrap.List):
        _namespace = 'LIST'

    def test(self):
        with rediswrap.PipelineContext() as pipe:
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
        self.assertEqual(members.result, [b'd', b'c', b'b', b'a'])
        self.assertEqual(rpush.result, 5)
        self.assertEqual(llen.result, 5)
        self.assertEqual(lrange.result, [b'd', b'c', b'b', b'a', b'e'])
        self.assertEqual(rpop.result, b'e')
        self.assertEqual(lrem.result, 1)
        self.assertEqual(ltrim.result, 1)
        self.assertEqual(members_after_ltrim.result, [b'd', b'c'])
        self.assertEqual(lindex.result, b'c')
        self.assertEqual(lset.result, 1)
        self.assertEqual(lindex_after.result, b'a')
        self.assertEqual(lpop.result, b'd')


class SortedSetTestCase(BaseTestCase):
    class Collection(rediswrap.SortedSet):
        _namespace = 'SORTEDSET'

    def test(self):
        with rediswrap.PipelineContext() as pipe:
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
            self.assertRaises(
                rediswrap.InvalidOperation,
                lambda: c.zadd('4', 4, xx=True, nx=True))
            c.delete()
            zrange = c.zrange(0, -1)
            self.assertRaises(AttributeError, lambda: members.result)
        self.assertEqual(add.result, 1)
        self.assertEqual(zaddincr.result, 5)
        self.assertEqual(zscore_after_incr.result, 5)
        self.assertEqual(zaddnx.result, 0)
        self.assertEqual(zaddxx.result, 0)
        self.assertEqual(zaddch.result, 1)
        self.assertEqual(zscore.result, 4.3)
        self.assertEqual(remove.result, 1)
        self.assertEqual(members.result, [b'2', b'3'])
        self.assertEqual(zaddmulti.result, 2)
        self.assertEqual(zrange.result, [])
        self.assertEqual(zincrby.result, 7.0)
        self.assertEqual(zrevrank.result, 0)
        self.assertEqual(zrevrange.result, [b'5', b'4'])

        with rediswrap.PipelineContext() as pipe:
            c = self.Collection('1')
            c.zadd('a', 1)
            c.zadd('b', 2)
            zrangebyscore = c.zrangebyscore(0, 10, start=0, num=1)
            zrevrangebyscore = c.zrevrangebyscore(10, 0, start=0, num=1)
            zcard = c.zcard()
            zrank = c.zrank('b')
            zremrangebyrank = c.zremrangebyrank(0, 0)
            zremrangebyscore = c.zremrangebyscore(2, 2)

        self.assertEqual(zrangebyscore.result, [b'a'])
        self.assertEqual(zrevrangebyscore.result, [b'b'])
        self.assertEqual(zcard.result, 2)
        self.assertEqual(zrank.result, 1)
        self.assertEqual(zremrangebyrank.result, 1)
        self.assertEqual(zremrangebyscore.result, 1)


class HashTestCase(BaseTestCase):
    class Collection(rediswrap.Hash):
        _namespace = 'HASH'

    def test(self):
        with rediswrap.PipelineContext() as pipe:
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
        self.assertEqual(hget.result, b'1')
        self.assertEqual(
            hgetall.result,
            {b'a': b'1', b'd': b'4', b'b': b'2', b'c': b'3'})
        self.assertEqual(hlen.result, 4)
        self.assertEqual(hdel.result, 2)
        self.assertEqual(set(hkeys.result), {b'c', b'd'})
        self.assertTrue(hexists.result)
        self.assertEqual(hincrby.result, 6)
        self.assertEqual(hmget.result, [b'3', b'6'])
        self.assertEqual(set(hvals.result), {b'3', b'6'})


if __name__ == '__main__':
    unittest.main(verbosity=2)
