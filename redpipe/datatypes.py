from .pipeline import pipeline
from .luascripts import lua_restorenx, lua_object_info
from .exceptions import InvalidOperation, InvalidFieldValue
from .result import Deferred
import re

__all__ = """
String
Set
List
SortedSet
Hash
""".split()


class DataType(object):
    """
    Base class for all datatypes.
    This class should not be used directly.
    """
    __slots__ = ['key', '_pipe']

    _keyspace = None
    _connection = None

    def __init__(self, key, pipe=None):
        """
        Pass in the key identifier for your object. This should not
        include the namespace. For example if you pass in 'foo', and
        your class has the namespace of A, when we talk to redis, the
        key will be: `A{foo}`.

        This allows for consistent namespacing and iteration through
        all keys in the namespace.

        :param key: str The name of your key.
        :param pipe: optional Pipeline or NestedPipeline
        """
        self.key = key
        self._pipe = pipe

    @staticmethod
    def _encode(v):
        return None if v is None else v.encode('utf-8')

    @staticmethod
    def _decode(v):
        return None if v is None else v.decode()

    @property
    def redis_key(self):
        """
        Get the key we pass to redis.
        If no namespace is declared, it will use the class name.
        :return: str
        """
        if self._keyspace is None:
            return self._encode("%s" % self.key)

        return self._encode("%s{%s}" % (self._keyspace, self.key))

    @property
    def pipe(self):
        """
        Get a fresh pipeline() to be used in a `with` block.
        :return: pipeline(autocommit=True)
        """
        return pipeline(self._pipe, name=self._connection, autocommit=True)

    def delete(self):
        """
        Remove the key from redis
        :return: Deferred()
        """
        with self.pipe as pipe:
            return pipe.delete(self.redis_key)

    def expire(self, time):
        """
        Allow the key to expire after ``time`` seconds.
        :param time: time expressed in seconds.
        :return: Deferred()
        """
        with self.pipe as pipe:
            return pipe.expire(self.redis_key, time)

    def exists(self):
        """
        does the key exist in redis?
        :return: Deferred()
        """
        with self.pipe as pipe:
            return pipe.exists(self.redis_key)

    def eval(self, script, *args):
        """
        Run a lua script against the key.
        :param script: str  A lua script targeting the current key.
        :param args: arguments to be passed to redis for the lua script
        :return: Deferred()
        """
        with self.pipe as pipe:
            return pipe.eval(script, 1, self.redis_key, *args)

    def dump(self):
        """
        get a redis RDB-like serialization of the object.
        :return: Deferred()
        """
        with self.pipe as pipe:
            return pipe.dump(self.redis_key)

    def restore(self, data, pttl=0):
        """
        Restore serialized dump of a key back into redis
        :param data: redis RDB-like serialization
        :param pttl: how many milliseconds till expiration of the key.
        :return: Deferred()
        """
        return self.eval(lua_restorenx, pttl, data)

    def ttl(self):
        """
        get the number of seconds until the key's expiration
        :return: Deferred()
        """
        with self.pipe as pipe:
            return pipe.ttl(self.redis_key)

    def persist(self):
        """
        clear any expiration TTL set on the object
        :return: Deferred()
        """
        with self.pipe as pipe:
            return pipe.persist(self.redis_key)

    def object(self, subcommand):
        """
        get the key's info stats
        :param subcommand: REFCOUNT | ENCODING | IDLETIME
        :return: Deferred()
        """
        return self.eval(lua_object_info, subcommand)

    def __str__(self):
        """
        A string representation of the Collection
        :return: str
        """
        return "<%s:%s>" % (self.__class__.__name__, self.key)

    @classmethod
    def scan(cls, cursor=0, match=None, count=None, pipe=None):
        """
        Incrementally return lists of key names. Also return a cursor
        indicating the scan position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        d = Deferred()
        if cls._keyspace is None:
            with pipeline(pipe, name=cls._connection, autocommit=True) as pipe:
                res = pipe.scan(cursor=cursor, match=match, count=count)

                def cb():
                    d.set((res[0], [cls._decode(v) for v in res[1]]))

                pipe.on_execute(cb)
                return d

        if match is None:
            match = '*'
        match = "%s{%s}" % (cls._keyspace, match)
        pattern = re.compile(r'^%s\{(.*)\}$' % cls._keyspace)

        with pipeline(pipe, name=cls._connection, autocommit=True) as pipe:

            res = pipe.scan(cursor=cursor, match=match, count=count)

            def cb():
                keys = []
                for k in res[1]:
                    k = cls._decode(k)
                    m = pattern.match(k)
                    if m:
                        keys.append(m.group(1))

                d.set((res[0], keys))

            pipe.on_execute(cb)
            return d

    @classmethod
    def scan_iter(cls, match=None, count=None):
        """
        Make an iterator using the SCAN command so that the client doesn't
        need to remember the cursor position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        cursor = '0'
        while cursor != 0:
            cursor, data = cls.scan(cursor=cursor, match=match, count=count)
            for item in data:
                yield item


class String(DataType):
    """
    Manipulate a String key in Redis.
    """

    def get(self):
        """
        Return the value of the key or None if the key doesn't exist
        :return: Deferred()
        """
        with self.pipe as pipe:
            d = Deferred()
            res = pipe.get(self.redis_key)

            def cb():
                d.set(self._decode(res.result))

            pipe.on_execute(cb)
            return d

    def set(self, value, ex=None, px=None, nx=False, xx=False):
        """
        Set the value at key ``name`` to ``value``

        ``ex`` sets an expire flag on key ``name`` for ``ex`` seconds.

        ``px`` sets an expire flag on key ``name`` for ``px`` milliseconds.

        ``nx`` if set to True, set the value at key ``name`` to ``value`` if it
            does not already exist.

        ``xx`` if set to True, set the value at key ``name`` to ``value`` if it
            already exists.

        :return: Deferred()
        """
        with self.pipe as pipe:
            return pipe.set(self.redis_key, self._encode(value),
                            ex=ex, px=px, nx=nx, xx=xx)

    def setnx(self, value):
        """
        Set the value as a string in the key only if the key doesn't exist.
        :param value:
        :return: Deferred()
        """
        with self.pipe as pipe:
            return pipe.setnx(self.redis_key, self._encode(value))

    def setex(self, value, time):
        """
        Set the value of key to ``value`` that expires in ``time``
        seconds. ``time`` can be represented by an integer or a Python
        timedelta object.
        """
        with self.pipe as pipe:
            return pipe.setex(self.redis_key, time, self._encode(value))

    def append(self, value):
        """
        Appends the string ``value`` to the value at ``key``. If ``key``
        doesn't already exist, create it with a value of ``value``.
        Returns the new length of the value at ``key``.
        :return: Deferred()
        """
        with self.pipe as pipe:
            return pipe.append(self.redis_key, self._encode(value))

    def strlen(self):
        """
        Return the number of bytes stored in the value of the key
        :param name:
        :return: Deferred()
        """
        with self.pipe as pipe:
            return pipe.strlen(self.redis_key)

    def substr(self, start, end=-1):
        """
        Return a substring of the string at key ``name``. ``start`` and ``end``
        are 0-based integers specifying the portion of the string to return.
        :param start: int
        :param end: int
        :return: Deferred()
        """
        with self.pipe as pipe:
            d = Deferred()
            res = pipe.substr(self.redis_key, start=start, end=end)

            def cb():
                d.set(self._decode(res.result))

            pipe.on_execute(cb)
            return d

    def setrange(self, offset, value):
        """
        Overwrite bytes in the value of ``name`` starting at ``offset`` with
        ``value``. If ``offset`` plus the length of ``value`` exceeds the
        length of the original value, the new value will be larger
        than before.
        If ``offset`` exceeds the length of the original value, null bytes
        will be used to pad between the end of the previous value and the start
        of what's being injected.

        Returns the length of the new string.
        """
        with self.pipe as pipe:
            return pipe.setrange(self.redis_key, offset, value)

    def setbit(self, offset, value):
        """
        Flag the ``offset`` in the key as ``value``. Returns a boolean
        indicating the previous value of ``offset``.
        :param  offset: int
        :param value:
        :return: Deferred()
        """
        with self.pipe as pipe:
            return pipe.setbit(self.redis_key, offset, value)

    def getbit(self, offset):
        """
        Returns a boolean indicating the value of ``offset`` in key
        """
        with self.pipe as pipe:
            return pipe.getbit(self.redis_key, offset)

    def bitcount(self, start=None, end=None):
        """
        Returns the count of set bits in the value of ``key``.  Optional
        ``start`` and ``end`` paramaters indicate which bytes to consider
        :param start: int
        :param end: int
        :return: Deferred()
        """
        with self.pipe as pipe:
            return pipe.bitcount(self.redis_key, start=start, end=end)

    def incr(self, amount=1):
        """
        increment the value for key by 1
        :return: Deferred()
        """
        with self.pipe as pipe:
            return pipe.incr(self.redis_key, amount=amount)

    def incrby(self, amount=1):
        """
        increment the value for key by value: int
        :param value: int
        :return: Deferred()
        """
        with self.pipe as pipe:
            return pipe.incrby(self.redis_key, amount=amount)

    def incrbyfloat(self, amount=1.0):
        """
        increment the value for key by value: float
        :param value: int
        :return: Deferred()
        """
        with self.pipe as pipe:
            return pipe.incrbyfloat(self.redis_key, amount=amount)


def _parse_values(values):
    (_values,) = values if len(values) == 1 else (None,)
    if _values and isinstance(_values, list):
        return _values
    return values


class Set(DataType):
    """
    Manipulate a Set key in redis.
    """

    def sadd(self, *values):
        """
        Add the specified members to the Set.
        :param values: a list of values or a simple value.
        :return: Deferred()
        """
        with self.pipe as pipe:
            values = [self._encode(v) for v in _parse_values(values)]
            return pipe.sadd(self.redis_key, *values)

    def srem(self, *values):
        """
        Remove the values from the Set if they are present.

        :param values: a list of values or a simple value.
        :return: Deferred()
        """
        with self.pipe as pipe:
            values = [self._encode(v) for v in _parse_values(values)]
            return pipe.srem(self.redis_key, *values)

    def spop(self):
        """
        Remove and return (pop) a random element from the Set.

        :return: Deferred()
        """
        with self.pipe as pipe:
            d = Deferred()
            res = pipe.spop(self.redis_key)

            def cb():
                d.set(self._decode(res.result))

            pipe.on_execute(cb)
            return d

    def smembers(self):
        with self.pipe as pipe:
            d = Deferred()
            res = pipe.smembers(self.redis_key)

            def cb():
                d.set({self._decode(v) for v in res.result})

            pipe.on_execute(cb)
            return d

    members = smembers

    def scard(self):
        """
        How many items in the set?

        :return: Deferred()

        """
        with self.pipe as pipe:
            return pipe.scard(self.redis_key)

    def sismember(self, value):
        """
        Is the provided value is in the ``Set`?
        :param value:
        :return: Deferred()
        """
        with self.pipe as pipe:
            return pipe.sismember(self.redis_key, self._encode(value))

    def srandmember(self):
        """
        Return a random member of the set.
        :return: Deferred()
        """
        with self.pipe as pipe:
            d = Deferred()
            res = pipe.srandmember(self.redis_key)

            def cb():
                d.set(self._decode(res.result))

            pipe.on_execute(cb)
            return d

    def sscan(self, cursor=0, match=None, count=None):
        """
        Incrementally return lists of elements in a set. Also return a cursor
        indicating the scan position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        with self.pipe as pipe:
            d = Deferred()
            res = pipe.sscan(self.redis_key, cursor=cursor,
                             match=match, count=count)

            def cb():
                d.set((res[0], [self._decode(v) for v in res[1]]))

            pipe.on_execute(cb)
            return d

    def sscan_iter(self, match=None, count=None):
        """
        Make an iterator using the SSCAN command so that the client doesn't
        need to remember the cursor position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        if self._pipe is not None:
            raise InvalidOperation('cannot pipeline scan operations')

        cursor = '0'
        while cursor != 0:
            cursor, data = self.sscan(cursor=cursor,
                                      match=match, count=count)
            for item in data:
                yield item

    add = sadd
    pop = spop
    remove = srem


class List(DataType):
    """
    Manipulate a List key in redis
    """

    def members(self):
        """
        Returns all items in the list.
        :return: Deferred()
        """
        return self.lrange(0, -1)

    def llen(self):
        """
        Returns the length of the list.
        :return: Deferred()
        """
        with self.pipe as pipe:
            return pipe.llen(self.redis_key)

    def lrange(self, start, stop):
        """
        Returns a range of items.

        :param start: integer representing the start index of the range
        :param stop: integer representing the size of the list.
        :return: Deferred()
        """
        with self.pipe as pipe:
            d = Deferred()
            res = pipe.lrange(self.redis_key, start, stop)

            def cb():
                d.set([self._decode(v) for v in res.result])

            pipe.on_execute(cb)
            return d

    def lpush(self, *values):
        """
        Push the value into the list from the *left* side

        :param values: a list of values or single value to push
        :return: Deferred()
        """
        with self.pipe as pipe:
            values = [self._encode(v) for v in _parse_values(values)]
            return pipe.lpush(self.redis_key, *values)

    def rpush(self, *values):
        """
        Push the value into the list from the *right* side

        :param values: a list of values or single value to push
        :return: Deferred()
        """
        with self.pipe as pipe:
            values = [self._encode(v) for v in _parse_values(values)]
            return pipe.rpush(self.redis_key, *values)

    def lpop(self):
        """
        Pop the first object from the left.

        :return: Deferred()

        """
        with self.pipe as pipe:
            d = Deferred()
            res = pipe.lpop(self.redis_key)

            def cb():
                d.set(self._decode(res.result))

            pipe.on_execute(cb)
            return d

    def rpop(self):
        """
        Pop the first object from the right.

        :return: the popped value.
        """
        with self.pipe as pipe:
            d = Deferred()
            res = pipe.rpop(self.redis_key)

            def cb():
                d.set(self._decode(res.result))

            pipe.on_execute(cb)
            return d

    def lrem(self, value, num=1):
        """
        Remove first occurrence of value.
        :param num:
        :param value:
        :return: Deferred()
        """
        with self.pipe as pipe:
            value = self._encode(value)
            return pipe.lrem(self.redis_key, num, value)

    def ltrim(self, start, end):
        """
        Trim the list from start to end.

        :param start:
        :param end:
        :return: Deferred()
        """
        with self.pipe as pipe:
            return pipe.ltrim(self.redis_key, start, end)

    def lindex(self, idx):
        """
        Return the value at the index *idx*

        :param idx: the index to fetch the value.
        :return: Deferred()
        """
        with self.pipe as pipe:
            d = Deferred()
            res = pipe.lindex(self.redis_key, idx)

            def cb():
                d.set(self._decode(res.result))

            pipe.on_execute(cb)
            return d

    def lset(self, idx, value):
        """
        Set the value in the list at index *idx*

        :param value:
        :param idx:
        :return: Deferred()
        """
        with self.pipe as pipe:
            value = self._encode(value)
            return pipe.lset(self.redis_key, idx, value)

    # noinspection PyRedeclaration
    remove = lrem
    trim = ltrim
    shift = lpop
    unshift = lpush
    pop = rpop
    push = rpush
    append = rpush


class SortedSet(DataType):
    """
    Manipulate a SortedSet key in redis.
    """

    def members(self):
        """
        Returns the members of the set.
        :return: Deferred()
        """
        return self.zrange(0, -1)

    def zadd(self, members, score=1, nx=False, xx=False, ch=False, incr=False):
        """
        Add members in the set and assign them the score.
        :param members: a list of item or a single item
        :param score: the score the assign to the item(s)
        :param nx:
        :param xx:
        :param ch:
        :param incr:
        :return: Deferred()
        """

        if nx:
            _args = ['NX']
        elif xx:
            _args = ['XX']
        else:
            _args = []

        if ch:
            _args.append('CH')

        if incr:
            _args.append('INCR')

        if isinstance(members, dict):
            for member, score in members.items():
                _args += [score, self._encode(member)]
        else:
            _args += [score, self._encode(members)]

        if nx and xx:
            raise InvalidOperation('cannot specify nx and xx at the same time')
        with self.pipe as pipe:
            return pipe.execute_command('ZADD', self.redis_key, *_args)

    def zrem(self, *values):
        """
        Remove the values from the SortedSet
        :param values:
        :return: True if **at least one** value is successfully
                 removed, False otherwise
        """
        with self.pipe as pipe:
            values = [self._encode(v) for v in _parse_values(values)]
            return pipe.zrem(self.redis_key, *values)

    def zincrby(self, member, increment):
        """
        Increment the score of the item by `value`
        :param member:
        :param increment:
        :return:
        """
        with self.pipe as pipe:
            return pipe.zincrby(self.redis_key,
                                self._encode(member), increment)

    def zrevrank(self, member):
        """
        Returns the ranking in reverse order for the member
        :param member: str
        """
        with self.pipe as pipe:
            return pipe.zrevrank(self.redis_key, self._encode(member))

    def zrange(self, start, end, desc=False, withscores=False,
               score_cast_func=float):
        """
        Returns all the elements including between ``start`` (non included)
        and ``stop`` (included).
        :param start:
        :param end:
        :param desc:
        :param withscores:
        :param score_cast_func:
        :return:
        """
        with self.pipe as pipe:
            d = Deferred()
            res = pipe.zrange(
                self.redis_key, start, end, desc=desc,
                withscores=withscores, score_cast_func=score_cast_func)

            def cb():
                if withscores:
                    d.set([[self._decode(v), s] for v, s in res.result])
                else:
                    d.set([self._decode(v) for v in res.result])

            pipe.on_execute(cb)
            return d

    def zrevrange(self, start, end,
                  withscores=False, score_cast_func=float):
        """
        Returns the range of items included between ``start`` and ``stop``
        in reverse order (from high to low)
        :param start:
        :param end:
        :param withscores:
        :param score_cast_func:
        :return:
        """
        with self.pipe as pipe:
            d = Deferred()
            res = pipe.zrevrange(self.redis_key, start, end,
                                 withscores=withscores,
                                 score_cast_func=score_cast_func)

            def cb():
                if withscores:
                    d.set([[self._decode(v), s] for v, s in res.result])
                else:
                    d.set([self._decode(v) for v in res.result])

            pipe.on_execute(cb)
            return d

    # noinspection PyShadowingBuiltins
    def zrangebyscore(self, min, max, start=None, num=None,
                      withscores=False, score_cast_func=float):
        """
        Returns the range of elements included between the scores (min and max)
        :param min:
        :param max:
        :param start:
        :param num:
        :param withscores:
        :param score_cast_func:
        :return: Deferred()
        """
        with self.pipe as pipe:
            d = Deferred()
            res = pipe.zrangebyscore(self.redis_key, min, max,
                                     start=start, num=num,
                                     withscores=withscores,
                                     score_cast_func=score_cast_func)

            def cb():
                if withscores:
                    d.set([[self._decode(v), s] for v, s in res.result])
                else:
                    d.set([self._decode(v) for v in res.result])

            pipe.on_execute(cb)
            return d

    # noinspection PyShadowingBuiltins
    def zrevrangebyscore(self, max, min, start=None, num=None,
                         withscores=False, score_cast_func=float):
        """
        Returns the range of elements between the scores (min and max).

        If ``start`` and ``num`` are specified, then return a slice
        of the range.

        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs

        `score_cast_func`` a callable used to cast the score return value
        :param max: int
        :param min: int
        :param start: int
        :param num: int
        :param withscores: bool
        :param score_cast_func:
        :return: Deferred()
        """
        with self.pipe as pipe:
            d = Deferred()
            res = pipe.zrevrangebyscore(self.redis_key, max, min, start=start,
                                        num=num, withscores=withscores,
                                        score_cast_func=score_cast_func)

            def cb():
                if withscores:
                    d.set([[self._decode(v), s] for v, s in res.result])
                else:
                    d.set([self._decode(v) for v in res.result])

            pipe.on_execute(cb)
            return d

    def zcard(self):
        """
        Returns the cardinality of the SortedSet.
        :return: Deferred()
        """
        with self.pipe as pipe:
            return pipe.zcard(self.redis_key)

    def zscore(self, elem):
        """
        Return the score of an element
        :param elem:
        :return: Deferred()
        """
        with self.pipe as pipe:
            return pipe.zscore(self.redis_key, self._encode(elem))

    def zremrangebyrank(self, start, stop):
        """
        Remove a range of element between the rank ``start`` and
        ``stop`` both included.
        :param stop:
        :param start:
        :return: Deferred()
        """
        with self.pipe as pipe:
            return pipe.zremrangebyrank(self.redis_key, start, stop)

    def zremrangebyscore(self, min_value, max_value):
        """
        Remove a range of element by between score ``min_value`` and
        ``max_value`` both included.
        :param max_value:
        :param min_value:
        :return: Deferred()
        """
        with self.pipe as pipe:
            return pipe.zremrangebyscore(self.redis_key, min_value, max_value)

    def zrank(self, elem):
        """
        Returns the rank of the element.
        :param elem:
        """
        with self.pipe as pipe:
            return pipe.zrank(self.redis_key, elem)

    def zscan(self, cursor=0, match=None, count=None,
              score_cast_func=float):
        """
        Incrementally return lists of elements in a sorted set. Also return a
        cursor indicating the scan position.

        ``match`` allows for filtering the members by pattern

        ``count`` allows for hint the minimum number of returns

        ``score_cast_func`` a callable used to cast the score return value
        """
        with self.pipe as pipe:
            d = Deferred()
            res = pipe.zscan(self.redis_key, cursor=cursor,
                             match=match, count=count, score_cast_func=float)

            def cb():
                d.set((res[0], [(self._decode(k), v) for k, v in res[1]]))

            pipe.on_execute(cb)
            return d

    def zscan_iter(self, match=None, count=None,
                   score_cast_func=float):
        """
        Make an iterator using the ZSCAN command so that the client doesn't
        need to remember the cursor position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns

        ``score_cast_func`` a callable used to cast the score return value
        """
        if self._pipe is not None:
            raise InvalidOperation('cannot pipeline scan operations')
        cursor = '0'
        while cursor != 0:
            cursor, data = self.zscan(cursor=cursor, match=match,
                                      count=count,
                                      score_cast_func=score_cast_func)
            for item in data:
                yield item

    revrank = zrevrank
    score = zscore
    rank = zrank
    incr_by = zincrby
    add = zadd
    remove = zrem


class Hash(DataType):
    """
    Manipulate a Hash key in Redis.
    """

    _fields = {}

    def to_redis(self, k, v):
        try:
            field_validator = self._fields[k]
            if not field_validator.validate(v):
                raise InvalidFieldValue('invalid value for field %s' % k)
            return self._encode(field_validator.to_persistence(v))
        except KeyError:
            return self._encode(v)

    def from_redis(self, k, v):
        try:
            field_validator = self._fields[k]
            return field_validator.from_persistence(self._decode(v))
        except KeyError:
            return self._decode(v)

    def hlen(self):
        """
        Returns the number of elements in the Hash.
        :return: Deferred()
        """
        with self.pipe as pipe:
            return pipe.hlen(self.redis_key)

    def hset(self, member, value):
        """
        Set ``member`` in the Hash at ``value``.

        :param value:
        :param member:
        :return: Deferred()
        """
        with self.pipe as pipe:
            value = self.to_redis(member, value)
            return pipe.hset(self.redis_key, self._encode(member), value)

    def hsetnx(self, member, value):
        """
        Set ``member`` in the Hash at ``value``.

        :param value:
        :param member:
        :return: Deferred()
        """
        with self.pipe as pipe:
            value = self.to_redis(member, value)
            return pipe.hsetnx(self.redis_key, self._encode(member), value)

    def hdel(self, *members):
        """
        Delete one or more hash field.

        :param members: on or more fields to remove.
        :return: Deferred()
        """
        with self.pipe as pipe:
            members = [self._encode(m) for m in _parse_values(members)]
            return pipe.hdel(self.redis_key, *members)

    def hkeys(self):
        """
        Returns all fields name in the Hash
        return: Deferred()
        """
        with self.pipe as pipe:
            d = Deferred()
            res = pipe.hkeys(self.redis_key)

            def cb():
                d.set([self._decode(v) for v in res.result])

            pipe.on_execute(cb)
            return d

    def hgetall(self):
        """
        Returns all the fields and values in the Hash.
        :return: Deferred()
        """
        with self.pipe as pipe:
            d = Deferred()
            res = pipe.hgetall(self.redis_key)

            def cb():
                data = {}
                for k, v in res.result.items():
                    k = self._decode(k)
                    v = self.from_redis(k, v)
                    data[k] = v
                d.set(data)

            pipe.on_execute(cb)
            return d

    def hvals(self):
        """
        Returns all the values in the Hash
        Unfortunately we can't type cast these fields.
        it is a useless call anyway imho.
        :return: Deferred()
        """
        with self.pipe as pipe:
            d = Deferred()
            res = pipe.hvals(self.redis_key)

            def cb():
                d.set([self._decode(v) for v in res.result])

            pipe.on_execute(cb)
            return d

    def hget(self, field):
        """
        Returns the value stored in the field, None if the field doesn't exist.
        :param field:
        :return: Deferred()
        """
        with self.pipe as pipe:
            d = Deferred()
            res = pipe.hget(self.redis_key, self._encode(field))

            def cb():
                d.set(self.from_redis(field, res.result))

            pipe.on_execute(cb)
            return d

    def hexists(self, field):
        """
        Returns ``True`` if the field exists, ``False`` otherwise.
        :param field:
        :return: Deferred()
        """
        with self.pipe as pipe:
            return pipe.hexists(self.redis_key, self._encode(field))

    def hincrby(self, field, increment=1):
        """
        Increment the value of the field.
        :param increment:
        :param field:
        :return: Deferred()
        """
        with self.pipe as pipe:
            return pipe.hincrby(self.redis_key, self._encode(field), increment)

    def hincrbyfloat(self, field, increment=1):
        """
        Increment the value of the field.
        :param increment:
        :param field:
        :return: Deferred()
        """
        with self.pipe as pipe:
            return pipe.hincrbyfloat(self.redis_key, self._encode(field),
                                     increment)

    def hmget(self, fields):
        """
        Returns the values stored in the fields.
        :param fields:
        :return: Deferred()
        """
        with self.pipe as pipe:
            d = Deferred()
            res = pipe.hmget(self.redis_key, [self._encode(f) for f in fields])

            def cb():
                d.set([self.from_redis(fields[i], v)
                       for i, v in enumerate(res.result)])

            pipe.on_execute(cb)
            return d

    def hmset(self, mapping):
        """
        Sets or updates the fields with their corresponding values.
        :param mapping: a dict with keys and values
        :return: Deferred()
        """
        with self.pipe as pipe:
            mapping = {self._encode(k): self.to_redis(k, v)
                       for k, v in mapping.items()}
            return pipe.hmset(self.redis_key, mapping)

    def hscan(self, cursor=0, match=None, count=None):
        """
        Incrementally return key/value slices in a hash. Also return a cursor
        indicating the scan position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        with self.pipe as pipe:
            d = Deferred()
            res = pipe.hscan(self.redis_key, cursor=cursor,
                             match=match, count=count)

            def cb():
                d.set((res[0], {self._decode(k): self._decode(v)
                                for k, v in res[1].items()}))

            pipe.on_execute(cb)
            return d

    def hscan_iter(self, match=None, count=None):
        """
        Make an iterator using the HSCAN command so that the client doesn't
        need to remember the cursor position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        if self._pipe is not None:
            raise InvalidOperation('cannot pipeline scan operations')
        cursor = '0'
        while cursor != 0:
            cursor, data = self.hscan(cursor=cursor,
                                      match=match, count=count)
            for item in data.items():
                yield item
