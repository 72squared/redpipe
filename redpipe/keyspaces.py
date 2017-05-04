# -*- coding: utf-8 -*-

"""
This module provides a way to access keys grouped under a certain keyspace.
A keyspace is a convention used often in redis where many keys are grouped
logically together.
In the SQL world, you could think of this as a table.
But in redis each key is independent whereas a record in a table is controlled
by the schema.

Examples of a group of keys in a keyspace:

* user{A}
* user{B}
* user{C}

It is inconvient to refer to keys this way.
The identifiers for our user records are A, B, C.
In addition, we usually know that a user record is always a redis hash.
And we know that it has certain fields that have different data types.

These keyspace classes in this module allow you to easily manipulate
these keys.

"""

from .pipelines import autoexec
from .luascripts import lua_restorenx, lua_object_info
from .exceptions import InvalidOperation
from .futures import Future
from .fields import TextField
import re

__all__ = """
String
Set
List
SortedSet
Hash
HyperLogLog
""".split()


class Keyspace(object):
    """
    Base class for all keyspace.
    This class should not be used directly.
    """
    __slots__ = ['key', '_pipe']

    _keyspace = None
    _connection = None
    _keyparse = TextField
    _valueparse = TextField

    def __init__(self, pipe=None):
        """
        Creates a new keyspace.
        Optionally pass in a pipeline object.
        If you pass in a pipeline, all commands to this instance will be
        pipelined.

        :param pipe: optional Pipeline or NestedPipeline
        """
        self._pipe = pipe

    @classmethod
    def redis_key(cls, key):
        """
        Get the key we pass to redis.
        If no namespace is declared, it will use the class name.

        :param name: str     the name of the redis key
        :return: str
        """
        keyspace = cls._keyspace
        key = "%s" % key if keyspace is None else "%s{%s}" % (keyspace, key)
        return cls._keyparse.encode(key)

    @property
    def pipe(self):
        """
        Get a fresh pipeline() to be used in a `with` block.

        :return: Pipeline or NestedPipeline with autoexec set to true.
        """
        return autoexec(self._pipe, name=self._connection)

    def delete(self, *names):
        """
        Remove the key from redis

        :param name: str     the name of the redis key
        :return: Future()
        """
        names = [self.redis_key(n) for n in names]
        with self.pipe as pipe:
            return pipe.delete(*names)

    def expire(self, name, time):
        """
        Allow the key to expire after ``time`` seconds.

        :param name: str     the name of the redis key
        :param time: time expressed in seconds.
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.expire(self.redis_key(name), time)

    def exists(self, name):
        """
        does the key exist in redis?

        :param name: str the name of the redis key
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.exists(self.redis_key(name))

    def eval(self, name, script, *args):
        """
        Run a lua script against the key.
        Doesn't support multi-key lua operations because
        we wouldn't be able to know what argument to namespace.
        Also, redis cluster doesn't really support multi-key operations.

        :param name: str     the name of the redis key
        :param script: str  A lua script targeting the current key.
        :param args: arguments to be passed to redis for the lua script
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.eval(script, 1, self.redis_key(name), *args)

    def dump(self, name):
        """
        get a redis RDB-like serialization of the object.

        :param name: str     the name of the redis key
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.dump(self.redis_key(name))

    def restorenx(self, name, value, pttl=0):
        """
        Restore serialized dump of a key back into redis

        :param name: str     the name of the redis key
        :param value: redis RDB-like serialization
        :param pttl: milliseconds till key expires
        :return: Future()
        """
        return self.eval(name, lua_restorenx, pttl, value)

    def restore(self, name, value, pttl=0):
        """
        Restore serialized dump of a key back into redis

        :param name: the name of the key
        :param value: the binary representation of the key.
        :param pttl: milliseconds till key expires
        :return:
        """
        with self.pipe as pipe:
            res = pipe.restore(self.redis_key(name), pttl, value)
            f = Future()

            def cb():
                f.set(self._valueparse.decode(res.result))

            pipe.on_execute(cb)
            return f

    def ttl(self, name):
        """
        get the number of seconds until the key's expiration

        :param name: str     the name of the redis key
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.ttl(self.redis_key(name))

    def persist(self, name):
        """
        clear any expiration TTL set on the object

        :param name: str     the name of the redis key
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.persist(self.redis_key(name))

    def pexpire(self, name, time):
        """
        Set an expire flag on key ``name`` for ``time`` milliseconds.
        ``time`` can be represented by an integer or a Python timedelta
        object.

        :param name: str
        :param time: int or timedelta
        :return Future
        """
        with self.pipe as pipe:
            return pipe.pexpire(self.redis_key(name), time)

    def pexpireat(self, name, when):
        """
        Set an expire flag on key ``name``. ``when`` can be represented
        as an integer representing unix time in milliseconds (unix time * 1000)
        or a Python datetime object.
        """
        with self.pipe as pipe:
            return pipe.pexpireat(self.redis_key(name), when)

    def pttl(self, name):
        """
        Returns the number of milliseconds until the key ``name`` will expire

        :param name: str    the name of the redis key
        :return:
        """
        with self.pipe as pipe:
            return pipe.pttl(self.redis_key(name))

    def rename(self, src, dst):
        """
        Rename key ``src`` to ``dst``
        """
        with self.pipe as pipe:
            return pipe.rename(self.redis_key(src), self.redis_key(dst))

    def renamenx(self, src, dst):
        "Rename key ``src`` to ``dst`` if ``dst`` doesn't already exist"
        with self.pipe as pipe:
            return pipe.renamenx(self.redis_key(src), self.redis_key(dst))

    def object(self, name, subcommand):
        """
        get the key's info stats

        :param name: str     the name of the redis key
        :param subcommand: REFCOUNT | ENCODING | IDLETIME
        :return: Future()
        """
        return self.eval(name, lua_object_info, subcommand)

    @classmethod
    def __str__(cls):
        """
        A string representation of the Collection

        :return: str
        """
        return "<%s>" % cls.__name__

    def scan(self, cursor=0, match=None, count=None):
        """
        Incrementally return lists of key names. Also return a cursor
        indicating the scan position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        f = Future()
        if self._keyspace is None:
            with self.pipe as pipe:
                res = pipe.scan(cursor=cursor, match=match, count=count)

                def cb():
                    f.set((res[0], [self._keyparse.decode(v) for v in res[1]]))

                pipe.on_execute(cb)
                return f

        if match is None:
            match = '*'
        match = "%s{%s}" % (self._keyspace, match)
        pattern = re.compile(r'^%s\{(.*)\}$' % self._keyspace)

        with self.pipe as pipe:

            res = pipe.scan(cursor=cursor, match=match, count=count)

            def cb():
                keys = []
                for k in res[1]:
                    k = self._keyparse.decode(k)
                    m = pattern.match(k)
                    if m:
                        keys.append(m.group(1))

                f.set((res[0], keys))

            pipe.on_execute(cb)
            return f

    def scan_iter(self, match=None, count=None):
        """
        Make an iterator using the SCAN command so that the client doesn't
        need to remember the cursor position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        if self._pipe is not None:
            raise InvalidOperation('cannot pipeline scan operations')

        cursor = '0'
        while cursor != 0:
            cursor, data = self.scan(cursor=cursor, match=match, count=count)
            for item in data:
                yield item

    def sort(self, name, start=None, num=None, by=None, get=None,
             desc=False, alpha=False, store=None, groups=False):
        """
        Sort and return the list, set or sorted set at ``name``.

        ``start`` and ``num`` allow for paging through the sorted data

        ``by`` allows using an external key to weight and sort the items.
            Use an "*" to indicate where in the key the item value is located

        ``get`` allows for returning items from external keys rather than the
            sorted data itself.  Use an "*" to indicate where int he key
            the item value is located

        ``desc`` allows for reversing the sort

        ``alpha`` allows for sorting lexicographically rather than numerically

        ``store`` allows for storing the result of the sort into
            the key ``store``

        ``groups`` if set to True and if ``get`` contains at least two
            elements, sort will return a list of tuples, each containing the
            values fetched from the arguments to ``get``.

        """
        with self.pipe as pipe:
            res = pipe.sort(self.redis_key(name), start=start, num=num,
                            by=by, get=get, desc=desc, alpha=alpha,
                            store=store, groups=groups)
            if store:
                return res
            f = Future()

            def cb():
                decode = self._valueparse.decode
                f.set([decode(v) for v in res.result])

            pipe.on_execute(cb)
            return f


class String(Keyspace):
    """
    Manipulate a String key in Redis.
    """

    def get(self, name):
        """
        Return the value of the key or None if the key doesn't exist

        :param name: str     the name of the redis key
        :return: Future()
        """
        with self.pipe as pipe:
            f = Future()
            res = pipe.get(self.redis_key(name))

            def cb():
                decode = self._valueparse.decode
                f.set(decode(res.result))

            pipe.on_execute(cb)
            return f

    def set(self, name, value, ex=None, px=None, nx=False, xx=False):
        """
        Set the value at key ``name`` to ``value``

        ``ex`` sets an expire flag on key ``name`` for ``ex`` seconds.

        ``px`` sets an expire flag on key ``name`` for ``px`` milliseconds.

        ``nx`` if set to True, set the value at key ``name`` to ``value`` if it
        does not already exist.

        ``xx`` if set to True, set the value at key ``name`` to ``value`` if it
        already exists.

        :param name: str     the name of the redis key
        :return: Future()
        """
        with self.pipe as pipe:
            value = self._valueparse.encode(value)
            return pipe.set(self.redis_key(name), value,
                            ex=ex, px=px, nx=nx, xx=xx)

    def setnx(self, name, value):
        """
        Set the value as a string in the key only if the key doesn't exist.

        :param name: str     the name of the redis key
        :param value:
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.setnx(self.redis_key(name),
                              self._valueparse.encode(value))

    def setex(self, name, value, time):
        """
        Set the value of key to ``value`` that expires in ``time``
        seconds. ``time`` can be represented by an integer or a Python
        timedelta object.

        :param name: str     the name of the redis key
        :param value: str
        :param time: secs
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.setex(self.redis_key(name), time,
                              self._valueparse.encode(value))

    def psetex(self, name, value, time_ms):
        """
        Set the value of key ``name`` to ``value`` that expires in ``time_ms``
        milliseconds. ``time_ms`` can be represented by an integer or a Python
        timedelta object
        """
        with self.pipe as pipe:
            return pipe.psetex(self.redis_key(name), time_ms, value)

    def append(self, name, value):
        """
        Appends the string ``value`` to the value at ``key``. If ``key``
        doesn't already exist, create it with a value of ``value``.
        Returns the new length of the value at ``key``.

        :param name: str     the name of the redis key
        :param value: str
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.append(self.redis_key(name),
                               self._valueparse.encode(value))

    def strlen(self, name):
        """
        Return the number of bytes stored in the value of the key

        :param name: str     the name of the redis key
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.strlen(self.redis_key(name))

    def substr(self, name, start, end=-1):
        """
        Return a substring of the string at key ``name``. ``start`` and ``end``
        are 0-based integers specifying the portion of the string to return.

        :param name: str     the name of the redis key
        :param start: int
        :param end: int
        :return: Future()
        """
        with self.pipe as pipe:
            f = Future()
            res = pipe.substr(self.redis_key(name), start=start, end=end)

            def cb():
                f.set(self._valueparse.decode(res.result))

            pipe.on_execute(cb)
            return f

    def setrange(self, name, offset, value):
        """
        Overwrite bytes in the value of ``name`` starting at ``offset`` with
        ``value``. If ``offset`` plus the length of ``value`` exceeds the
        length of the original value, the new value will be larger
        than before.
        If ``offset`` exceeds the length of the original value, null bytes
        will be used to pad between the end of the previous value and the start
        of what's being injected.

        Returns the length of the new string.
        :param name: str     the name of the redis key
        :param offset: int
        :param value: str
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.setrange(self.redis_key(name), offset, value)

    def setbit(self, name, offset, value):
        """
        Flag the ``offset`` in the key as ``value``. Returns a boolean
        indicating the previous value of ``offset``.

        :param name: str     the name of the redis key
        :param  offset: int
        :param value:
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.setbit(self.redis_key(name), offset, value)

    def getbit(self, name, offset):
        """
        Returns a boolean indicating the value of ``offset`` in key

        :param name: str     the name of the redis key
        :param offset: int
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.getbit(self.redis_key(name), offset)

    def bitcount(self, name, start=None, end=None):
        """
        Returns the count of set bits in the value of ``key``.  Optional
        ``start`` and ``end`` paramaters indicate which bytes to consider

        :param name: str     the name of the redis key
        :param start: int
        :param end: int
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.bitcount(self.redis_key(name), start=start, end=end)

    def incr(self, name, amount=1):
        """
        increment the value for key by 1

        :param name: str     the name of the redis key
        :param amount: int
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.incr(self.redis_key(name), amount=amount)

    def incrby(self, name, amount=1):
        """
        increment the value for key by value: int

        :param name: str     the name of the redis key
        :param amount: int
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.incrby(self.redis_key(name), amount=amount)

    def incrbyfloat(self, name, amount=1.0):
        """
        increment the value for key by value: float

        :param name: str     the name of the redis key
        :param value: int
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.incrbyfloat(self.redis_key(name), amount=amount)

    def __getitem__(self, name):
        """
        magic python method that makes the class behave like a dictionary.

        use to access elements.

        :param name:
        :return:
        """
        return self.get(name)

    def __setitem__(self, name, value):
        """
        magic python method that makes the class behave like a dictionary.

        use to set elements.

        :param name:
        :param value:
        :return:
        """
        self.set(name, value)


def _parse_values(values, extra=None):
    """
    Utility function to flatten out args.

    For internal use only.

    :param values: list, tuple, or str
    :param extra: list or None
    :return: list
    """
    coerced = list(values)

    if coerced == values:
        values = coerced
    else:
        coerced = tuple(values)
        if coerced == values:
            values = list(values)
        else:
            values = [values]

    if extra:
        values.extend(extra)
    return values


class Set(Keyspace):
    """
    Manipulate a Set key in redis.
    """

    def sdiff(self, keys, *args):
        """
        Return the difference of sets specified by ``keys``

        :param keys: list
        :param args: tuple
        :return: Future()
        """
        keys = [self.redis_key(k) for k in _parse_values(keys, args)]

        with self.pipe as pipe:
            res = pipe.sdiff(*keys)
            f = Future()

            def cb():
                f.set({self._valueparse.decode(v) for v in res.result})

            pipe.on_execute(cb)
            return f

    def sdiffstore(self, dest, *keys):
        """
        Store the difference of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.
        """
        keys = [self.redis_key(k) for k in _parse_values(keys)]

        with self.pipe as pipe:
            return pipe.sdiffstore(self.redis_key(dest), *keys)

    def sinter(self, keys, *args):
        """
        Return the intersection of sets specified by ``keys``

        :param keys: list or str
        :param args: tuple
        :return: Future
        """

        keys = [self.redis_key(k) for k in _parse_values(keys, args)]
        with self.pipe as pipe:
            res = pipe.sinter(*keys)
            f = Future()

            def cb():
                f.set({self._valueparse.decode(v) for v in res.result})

            pipe.on_execute(cb)
            return f

    def sinterstore(self, dest, keys, *args):
        """
        Store the intersection of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.
        """
        keys = [self.redis_key(k) for k in _parse_values(keys, args)]
        with self.pipe as pipe:
            return pipe.sinterstore(self.redis_key(dest), keys)

    def sunion(self, keys, *args):
        """
        Return the union of sets specified by ``keys``

        :param keys: list or str
        :param args: tuple
        :return: Future()
        """
        keys = [self.redis_key(k) for k in _parse_values(keys, args)]
        with self.pipe as pipe:
            res = pipe.sunion(*keys)
            f = Future()

            def cb():
                f.set({self._valueparse.decode(v) for v in res.result})

            pipe.on_execute(cb)
            return f

    def sunionstore(self, dest, keys, *args):
        """
        Store the union of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of members in the new set.
        """
        keys = [self.redis_key(k) for k in _parse_values(keys, args)]
        with self.pipe as pipe:
            return pipe.sunionstore(self.redis_key(dest), *keys)

    def sadd(self, name, values, *args):
        """
        Add the specified members to the Set.

        :param name: str     the name of the redis key
        :param values: a list of values or a simple value.
        :return: Future()
        """
        with self.pipe as pipe:
            values = [self._valueparse.encode(v) for v in
                      _parse_values(values, args)]
            return pipe.sadd(self.redis_key(name), *values)

    def srem(self, name, *values):
        """
        Remove the values from the Set if they are present.

        :param name: str     the name of the redis key
        :param values: a list of values or a simple value.
        :return: Future()
        """
        with self.pipe as pipe:
            v_encode = self._valueparse.encode
            values = [v_encode(v) for v in _parse_values(values)]
            return pipe.srem(self.redis_key(name), *values)

    def spop(self, name):
        """
        Remove and return (pop) a random element from the Set.

        :param name: str     the name of the redis key
        :return: Future()
        """
        with self.pipe as pipe:
            f = Future()
            res = pipe.spop(self.redis_key(name))

            def cb():
                f.set(self._valueparse.decode(res.result))

            pipe.on_execute(cb)
            return f

    def smembers(self, name):
        """
        get the set of all members for key

        :param name: str     the name of the redis key
        :return:
        """
        with self.pipe as pipe:
            f = Future()
            res = pipe.smembers(self.redis_key(name))

            def cb():
                f.set({self._valueparse.decode(v) for v in res.result})

            pipe.on_execute(cb)
            return f

    def scard(self, name):
        """
        How many items in the set?

        :param name: str     the name of the redis key
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.scard(self.redis_key(name))

    def sismember(self, name, value):
        """
        Is the provided value is in the ``Set``?

        :param name: str     the name of the redis key
        :param value: str
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.sismember(self.redis_key(name),
                                  self._valueparse.encode(value))

    def srandmember(self, name):
        """
        Return a random member of the set.

        :param name: str     the name of the redis key
        :return: Future()
        """
        with self.pipe as pipe:
            f = Future()
            res = pipe.srandmember(self.redis_key(name))

            def cb():
                f.set(self._valueparse.decode(res.result))

            pipe.on_execute(cb)
            return f

    def sscan(self, name, cursor=0, match=None, count=None):
        """
        Incrementally return lists of elements in a set. Also return a cursor
        indicating the scan position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns

        :param name: str     the name of the redis key
        :param cursor: int
        :param match: str
        :param count: int
        """
        with self.pipe as pipe:
            f = Future()
            res = pipe.sscan(self.redis_key(name), cursor=cursor,
                             match=match, count=count)

            def cb():
                f.set((res[0], [self._valueparse.decode(v) for v in res[1]]))

            pipe.on_execute(cb)
            return f

    def sscan_iter(self, name, match=None, count=None):
        """
        Make an iterator using the SSCAN command so that the client doesn't
        need to remember the cursor position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns

        :param name: str     the name of the redis key
        :param match: str
        :param count: int
        """
        if self._pipe is not None:
            raise InvalidOperation('cannot pipeline scan operations')

        cursor = '0'
        while cursor != 0:
            cursor, data = self.sscan(name, cursor=cursor,
                                      match=match, count=count)
            for item in data:
                yield item

    add = sadd
    pop = spop
    remove = srem


class List(Keyspace):
    """
    Manipulate a List key in redis
    """

    def blpop(self, keys, timeout=0):
        """
        LPOP a value off of the first non-empty list
        named in the ``keys`` list.

        If none of the lists in ``keys`` has a value to LPOP, then block
        for ``timeout`` seconds, or until a value gets pushed on to one
        of the lists.

        If timeout is 0, then block indefinitely.
        """
        map = {self.redis_key(k): k for k in _parse_values(keys)}
        keys = map.keys()

        with self.pipe as pipe:
            f = Future()
            res = pipe.blpop(keys, timeout=timeout)

            def cb():
                if res.result:
                    k = map[res.result[0]]
                    v = self._valueparse.decode(res.result[1])

                    f.set((k, v))
                else:
                    f.set(res.result)

            pipe.on_execute(cb)
            return f

    def brpop(self, keys, timeout=0):
        """
        RPOP a value off of the first non-empty list
        named in the ``keys`` list.

        If none of the lists in ``keys`` has a value to LPOP, then block
        for ``timeout`` seconds, or until a value gets pushed on to one
        of the lists.

        If timeout is 0, then block indefinitely.
        """
        map = {self.redis_key(k): k for k in _parse_values(keys)}
        keys = map.keys()

        with self.pipe as pipe:
            f = Future()
            res = pipe.brpop(keys, timeout=timeout)

            def cb():
                if res.result:
                    k = map[res.result[0]]
                    v = self._valueparse.decode(res.result[1])

                    f.set((k, v))
                else:
                    f.set(res.result)

            pipe.on_execute(cb)
            return f

    def brpoplpush(self, src, dst, timeout=0):
        """
        Pop a value off the tail of ``src``, push it on the head of ``dst``
        and then return it.

        This command blocks until a value is in ``src`` or until ``timeout``
        seconds elapse, whichever is first. A ``timeout`` value of 0 blocks
        forever.
        """
        with self.pipe as pipe:
            res = pipe.brpoplpush(self.redis_key(src),
                                  self.redis_key(dst), timeout)
            f = Future()

            def cb():
                f.set(self._valueparse.decode(res.result))

            pipe.on_execute(cb)
            return f

    def llen(self, name):
        """
        Returns the length of the list.

        :param name: str     the name of the redis key
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.llen(self.redis_key(name))

    def lrange(self, name, start, stop):
        """
        Returns a range of items.

        :param name: str     the name of the redis key
        :param start: integer representing the start index of the range
        :param stop: integer representing the size of the list.
        :return: Future()
        """
        with self.pipe as pipe:
            f = Future()
            res = pipe.lrange(self.redis_key(name), start, stop)

            def cb():
                f.set([self._valueparse.decode(v) for v in res.result])

            pipe.on_execute(cb)
            return f

    def lpush(self, name, *values):
        """
        Push the value into the list from the *left* side

        :param name: str     the name of the redis key
        :param values: a list of values or single value to push
        :return: Future()
        """
        with self.pipe as pipe:
            v_encode = self._valueparse.encode
            values = [v_encode(v) for v in _parse_values(values)]
            return pipe.lpush(self.redis_key(name), *values)

    def rpush(self, name, *values):
        """
        Push the value into the list from the *right* side

        :param name: str     the name of the redis key
        :param values: a list of values or single value to push
        :return: Future()
        """
        with self.pipe as pipe:
            v_encode = self._valueparse.encode
            values = [v_encode(v) for v in _parse_values(values)]
            return pipe.rpush(self.redis_key(name), *values)

    def lpop(self, name):
        """
        Pop the first object from the left.

        :param name: str     the name of the redis key
        :return: Future()

        """
        with self.pipe as pipe:
            f = Future()
            res = pipe.lpop(self.redis_key(name))

            def cb():
                f.set(self._valueparse.decode(res.result))

            pipe.on_execute(cb)
            return f

    def rpop(self, name):
        """
        Pop the first object from the right.

        :param name: str     the name of the redis key
        :return: the popped value.
        """
        with self.pipe as pipe:
            f = Future()
            res = pipe.rpop(self.redis_key(name))

            def cb():
                f.set(self._valueparse.decode(res.result))

            pipe.on_execute(cb)
            return f

    def rpoplpush(self, src, dst):
        """
        RPOP a value off of the ``src`` list and atomically LPUSH it
        on to the ``dst`` list.  Returns the value.
        """
        with self.pipe as pipe:
            f = Future()
            res = pipe.rpoplpush(self.redis_key(src), self.redis_key(dst))

            def cb():
                f.set(self._valueparse.decode(res.result))

            pipe.on_execute(cb)
            return f

    def lrem(self, name, value, num=1):
        """
        Remove first occurrence of value.

        :param name: str     the name of the redis key
        :param num:
        :param value:
        :return: Future()
        """
        with self.pipe as pipe:
            value = self._valueparse.encode(value)
            return pipe.lrem(self.redis_key(name), num, value)

    def ltrim(self, name, start, end):
        """
        Trim the list from start to end.

        :param name: str     the name of the redis key
        :param start:
        :param end:
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.ltrim(self.redis_key(name), start, end)

    def lindex(self, name, idx):
        """
        Return the value at the index *idx*

        :param name: str     the name of the redis key
        :param idx: the index to fetch the value.
        :return: Future()
        """
        with self.pipe as pipe:
            f = Future()
            res = pipe.lindex(self.redis_key(name), idx)

            def cb():
                f.set(self._valueparse.decode(res.result))

            pipe.on_execute(cb)
            return f

    def lset(self, name, idx, value):
        """
        Set the value in the list at index *idx*

        :param name: str     the name of the redis key
        :param value:
        :param idx:
        :return: Future()
        """
        with self.pipe as pipe:
            value = self._valueparse.encode(value)
            return pipe.lset(self.redis_key(name), idx, value)

    # noinspection PyRedeclaration
    remove = lrem
    trim = ltrim
    shift = lpop
    unshift = lpush
    pop = rpop
    push = rpush
    append = rpush


class SortedSet(Keyspace):
    """
    Manipulate a SortedSet key in redis.
    """

    def zadd(self, name, members, score=1, nx=False,
             xx=False, ch=False, incr=False):
        """
        Add members in the set and assign them the score.

        :param name: str     the name of the redis key
        :param members: a list of item or a single item
        :param score: the score the assign to the item(s)
        :param nx:
        :param xx:
        :param ch:
        :param incr:
        :return: Future()
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
                _args += [score, self._valueparse.encode(member)]
        else:
            _args += [score, self._valueparse.encode(members)]

        if nx and xx:
            raise InvalidOperation('cannot specify nx and xx at the same time')
        with self.pipe as pipe:
            return pipe.execute_command('ZADD', self.redis_key(name), *_args)

    def zrem(self, name, *values):
        """
        Remove the values from the SortedSet

        :param name: str     the name of the redis key
        :param values:
        :return: True if **at least one** value is successfully
                 removed, False otherwise
        """
        with self.pipe as pipe:
            v_encode = self._valueparse.encode
            values = [v_encode(v) for v in _parse_values(values)]
            return pipe.zrem(self.redis_key(name), *values)

    def zincrby(self, name, member, increment):
        """
        Increment the score of the item by `value`

        :param name: str     the name of the redis key
        :param member:
        :param increment:
        :return:
        """
        with self.pipe as pipe:
            return pipe.zincrby(self.redis_key(name),
                                self._valueparse.encode(member), increment)

    def zrevrank(self, name, member):
        """
        Returns the ranking in reverse order for the member

        :param name: str     the name of the redis key
        :param member: str
        """
        with self.pipe as pipe:
            return pipe.zrevrank(self.redis_key(name),
                                 self._valueparse.encode(member))

    def zrange(self, name, start, end, desc=False, withscores=False,
               score_cast_func=float):
        """
        Returns all the elements including between ``start`` (non included)
        and ``stop`` (included).

        :param name: str     the name of the redis key
        :param start:
        :param end:
        :param desc:
        :param withscores:
        :param score_cast_func:
        :return:
        """
        with self.pipe as pipe:
            f = Future()
            res = pipe.zrange(
                self.redis_key(name), start, end, desc=desc,
                withscores=withscores, score_cast_func=score_cast_func)

            def cb():
                if withscores:
                    f.set([[self._valueparse.decode(v), s] for v, s in
                           res.result])
                else:
                    f.set([self._valueparse.decode(v) for v in res.result])

            pipe.on_execute(cb)
            return f

    def zrevrange(self, name, start, end,
                  withscores=False, score_cast_func=float):
        """
        Returns the range of items included between ``start`` and ``stop``
        in reverse order (from high to low)

        :param name: str     the name of the redis key
        :param start:
        :param end:
        :param withscores:
        :param score_cast_func:
        :return:
        """
        with self.pipe as pipe:
            f = Future()
            res = pipe.zrevrange(self.redis_key(name), start, end,
                                 withscores=withscores,
                                 score_cast_func=score_cast_func)

            def cb():
                if withscores:
                    f.set([[self._valueparse.decode(v), s] for v, s in
                           res.result])
                else:
                    f.set([self._valueparse.decode(v) for v in res.result])

            pipe.on_execute(cb)
            return f

    # noinspection PyShadowingBuiltins
    def zrangebyscore(self, name, min, max, start=None, num=None,
                      withscores=False, score_cast_func=float):
        """
        Returns the range of elements included between the scores (min and max)

        :param name: str     the name of the redis key
        :param min:
        :param max:
        :param start:
        :param num:
        :param withscores:
        :param score_cast_func:
        :return: Future()
        """
        with self.pipe as pipe:
            f = Future()
            res = pipe.zrangebyscore(self.redis_key(name), min, max,
                                     start=start, num=num,
                                     withscores=withscores,
                                     score_cast_func=score_cast_func)

            def cb():
                if withscores:
                    f.set([[self._valueparse.decode(v), s] for v, s in
                           res.result])
                else:
                    f.set([self._valueparse.decode(v) for v in res.result])

            pipe.on_execute(cb)
            return f

    # noinspection PyShadowingBuiltins
    def zrevrangebyscore(self, name, max, min, start=None, num=None,
                         withscores=False, score_cast_func=float):
        """
        Returns the range of elements between the scores (min and max).

        If ``start`` and ``num`` are specified, then return a slice
        of the range.

        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs

        `score_cast_func`` a callable used to cast the score return value

        :param name: str     the name of the redis key
        :param max: int
        :param min: int
        :param start: int
        :param num: int
        :param withscores: bool
        :param score_cast_func:
        :return: Future()
        """
        with self.pipe as pipe:
            f = Future()
            res = pipe.zrevrangebyscore(self.redis_key(name), max, min,
                                        start=start, num=num,
                                        withscores=withscores,
                                        score_cast_func=score_cast_func)

            def cb():
                if withscores:
                    f.set([[self._valueparse.decode(v), s] for v, s in
                           res.result])
                else:
                    f.set([self._valueparse.decode(v) for v in res.result])

            pipe.on_execute(cb)
            return f

    def zcard(self, name):
        """
        Returns the cardinality of the SortedSet.

        :param name: str     the name of the redis key
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.zcard(self.redis_key(name))

    def zscore(self, name, elem):
        """
        Return the score of an element

        :param name: str     the name of the redis key
        :param elem:
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.zscore(self.redis_key(name),
                               self._valueparse.encode(elem))

    def zremrangebyrank(self, name, start, stop):
        """
        Remove a range of element between the rank ``start`` and
        ``stop`` both included.

        :param name: str     the name of the redis key
        :param stop:
        :param start:
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.zremrangebyrank(self.redis_key(name), start, stop)

    def zremrangebyscore(self, name, min_value, max_value):
        """
        Remove a range of element by between score ``min_value`` and
        ``max_value`` both included.

        :param name: str     the name of the redis key
        :param max_value:
        :param min_value:
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.zremrangebyscore(self.redis_key(name),
                                         min_value, max_value)

    def zrank(self, name, elem):
        """
        Returns the rank of the element.

        :param name: str     the name of the redis key
        :param elem:
        """
        with self.pipe as pipe:
            return pipe.zrank(self.redis_key(name), elem)

    # noinspection PyShadowingBuiltins
    def zlexcount(self, name, min, max):
        """
        Return the number of items in the sorted set between the
        lexicographical range ``min`` and ``max``.

        :param name: str     the name of the redis key
        :param min: int or '-inf'
        :param max: int or '+inf'
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.zlexcount(self.redis_key(name), min, max)

    # noinspection PyShadowingBuiltins
    def zrangebylex(self, name, min, max, start=None, num=None):
        """
        Return the lexicographical range of values from sorted set ``name``
        between ``min`` and ``max``.

        If ``start`` and ``num`` are specified, then return a slice of the
        range.

        :param name: str     the name of the redis key
        :param min: int or '-inf'
        :param max: int or '+inf'
        :param start: int
        :param num: int
        :return: Future()
        """
        with self.pipe as pipe:
            f = Future()
            res = pipe.zrangebylex(self.redis_key(name), min, max,
                                   start=start, num=num)

            def cb():
                f.set([self._valueparse.decode(v) for v in res])

            pipe.on_execute(cb)
            return f

    # noinspection PyShadowingBuiltins
    def zrevrangebylex(self, name, max, min, start=None, num=None):
        """
        Return the reversed lexicographical range of values from the sorted set
         between ``max`` and ``min``.

        If ``start`` and ``num`` are specified, then return a slice of the
        range.

        :param name: str     the name of the redis key
        :param max: int or '+inf'
        :param min: int or '-inf'
        :param start: int
        :param num: int
        :return: Future()
        """
        with self.pipe as pipe:
            f = Future()
            res = pipe.zrevrangebylex(self.redis_key(name), max, min,
                                      start=start, num=num)

            def cb():
                f.set([self._valueparse.decode(v) for v in res])

            pipe.on_execute(cb)
            return f

    # noinspection PyShadowingBuiltins
    def zremrangebylex(self, name, min, max):
        """
        Remove all elements in the sorted set between the
        lexicographical range specified by ``min`` and ``max``.

        Returns the number of elements removed.
        :param name: str     the name of the redis key
        :param min: int or -inf
        :param max: into or +inf
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.zremrangebylex(self.redis_key(name), min, max)

    def zunionstore(self, dest, keys, aggregate=None):
        """
        Union multiple sorted sets specified by ``keys`` into
        a new sorted set, ``dest``. Scores in the destination will be
        aggregated based on the ``aggregate``, or SUM if none is provided.
        """
        with self.pipe as pipe:
            keys = [self.redis_key(k) for k in keys]
            return pipe.zunionstore(self.redis_key(dest), keys,
                                    aggregate=aggregate)

    def zscan(self, name, cursor=0, match=None, count=None,
              score_cast_func=float):
        """
        Incrementally return lists of elements in a sorted set. Also return a
        cursor indicating the scan position.

        ``match`` allows for filtering the members by pattern

        ``count`` allows for hint the minimum number of returns

        ``score_cast_func`` a callable used to cast the score return value
        """
        with self.pipe as pipe:
            f = Future()
            res = pipe.zscan(self.redis_key(name), cursor=cursor,
                             match=match, count=count,
                             score_cast_func=score_cast_func)

            def cb():
                f.set((res[0], [(self._valueparse.decode(k), v)
                                for k, v in res[1]]))

            pipe.on_execute(cb)
            return f

    def zscan_iter(self, name, match=None, count=None,
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
            cursor, data = self.zscan(name, cursor=cursor, match=match,
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


class Hash(Keyspace):
    """
    Manipulate a Hash key in Redis.
    """

    _fields = {}

    _memberparse = TextField

    def _value_encode(self, member, value):
        """
        Internal method used to encode values into the hash.

        :param member: str
        :param value: multi
        :return: bytes
        """
        try:
            field_validator = self._fields[member]
        except KeyError:
            return self._valueparse.encode(value)

        return field_validator.encode(value)

    def _value_decode(self, member, value):
        """
        Internal method used to decode values from redis hash

        :param member: str
        :param value: bytes
        :return: multi
        """
        if value is None:
            return None
        try:
            field_validator = self._fields[member]
        except KeyError:
            return self._valueparse.decode(value)

        return field_validator.decode(value)

    def hlen(self, name):
        """
        Returns the number of elements in the Hash.

        :param name: str     the name of the redis key
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.hlen(self.redis_key(name))

    def hset(self, name, member, value):
        """
        Set ``member`` in the Hash at ``value``.

        :param name: str     the name of the redis key
        :param value:
        :param member:
        :return: Future()
        """
        with self.pipe as pipe:
            value = self._value_encode(member, value)
            member = self._memberparse.encode(member)
            return pipe.hset(self.redis_key(name), member, value)

    def hsetnx(self, name, member, value):
        """
        Set ``member`` in the Hash at ``value``.

        :param name: str     the name of the redis key
        :param value:
        :param member:
        :return: Future()
        """
        with self.pipe as pipe:
            value = self._value_encode(member, value)
            member = self._memberparse.encode(member)
            return pipe.hsetnx(self.redis_key(name), member, value)

    def hdel(self, name, *keys):
        """
        Delete one or more hash field.

        :param name: str     the name of the redis key
        :param keys: on or more members to remove from the key.
        :return: Future()
        """
        with self.pipe as pipe:
            m_encode = self._memberparse.encode
            keys = [m_encode(m) for m in _parse_values(keys)]
            return pipe.hdel(self.redis_key(name), *keys)

    def hkeys(self, name):
        """
        Returns all fields name in the Hash.

        :param name: str the name of the redis key
        :return: Future
        """
        with self.pipe as pipe:
            f = Future()
            res = pipe.hkeys(self.redis_key(name))

            def cb():
                m_decode = self._memberparse.decode
                f.set([m_decode(v) for v in res.result])

            pipe.on_execute(cb)
            return f

    def hgetall(self, name):
        """
        Returns all the fields and values in the Hash.

        :param name: str     the name of the redis key
        :return: Future()
        """
        with self.pipe as pipe:
            f = Future()
            res = pipe.hgetall(self.redis_key(name))

            def cb():
                data = {}
                m_decode = self._memberparse.decode
                v_decode = self._value_decode
                for k, v in res.result.items():
                    k = m_decode(k)
                    v = v_decode(k, v)
                    data[k] = v
                f.set(data)

            pipe.on_execute(cb)
            return f

    def hvals(self, name):
        """
        Returns all the values in the Hash
        Unfortunately we can't type cast these fields.
        it is a useless call anyway imho.

        :param name: str     the name of the redis key
        :return: Future()
        """
        with self.pipe as pipe:
            f = Future()
            res = pipe.hvals(self.redis_key(name))

            def cb():
                f.set([self._valueparse.decode(v) for v in res.result])

            pipe.on_execute(cb)
            return f

    def hget(self, name, field):
        """
        Returns the value stored in the field, None if the field doesn't exist.

        :param name: str     the name of the redis key
        :param field:
        :return: Future()
        """
        with self.pipe as pipe:
            f = Future()
            res = pipe.hget(self.redis_key(name),
                            self._memberparse.encode(field))

            def cb():
                f.set(self._value_decode(field, res.result))

            pipe.on_execute(cb)
            return f

    def hexists(self, name, field):
        """
        Returns ``True`` if the field exists, ``False`` otherwise.

        :param name: str     the name of the redis key
        :param field:
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.hexists(self.redis_key(name),
                                self._memberparse.encode(field))

    def hincrby(self, name, field, increment=1):
        """
        Increment the value of the field.

        :param name: str     the name of the redis key
        :param increment: int
        :param field: str
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.hincrby(self.redis_key(name),
                                self._memberparse.encode(field),
                                increment)

    def hincrbyfloat(self, name, field, increment=1):
        """
        Increment the value of the field.

        :param name: str     the name of the redis key
        :param increment:
        :param field:
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.hincrbyfloat(self.redis_key(name),
                                     self._memberparse.encode(field),
                                     increment)

    def hmget(self, name, keys):
        """
        Returns the values stored in the fields.

        :param name: str     the name of the redis key
        :param fields:
        :return: Future()
        """
        with self.pipe as pipe:
            f = Future()
            res = pipe.hmget(self.redis_key(name),
                             [self._memberparse.encode(k) for k in keys])

            def cb():
                f.set([self._value_decode(keys[i], v)
                       for i, v in enumerate(res.result)])

            pipe.on_execute(cb)
            return f

    def hmset(self, name, mapping):
        """
        Sets or updates the fields with their corresponding values.

        :param name: str     the name of the redis key
        :param mapping: a dict with keys and values
        :return: Future()
        """
        with self.pipe as pipe:
            m_encode = self._memberparse.encode
            mapping = {m_encode(k): self._value_encode(k, v)
                       for k, v in mapping.items()}
            return pipe.hmset(self.redis_key(name), mapping)

    def hscan(self, name, cursor=0, match=None, count=None):
        """
        Incrementally return key/value slices in a hash. Also return a cursor
        indicating the scan position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        with self.pipe as pipe:
            f = Future()
            res = pipe.hscan(self.redis_key(name), cursor=cursor,
                             match=match, count=count)

            def cb():
                data = {}
                m_decode = self._memberparse.decode
                for k, v in res[1].items():
                    k = m_decode(k)
                    v = self._value_decode(k, v)
                    data[k] = v

                f.set((res[0], data))

            pipe.on_execute(cb)
            return f

    def hscan_iter(self, name, match=None, count=None):
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
            cursor, data = self.hscan(name, cursor=cursor,
                                      match=match, count=count)
            for item in data.items():
                yield item


class HyperLogLog(Keyspace):
    """
    Manipulate a HyperLogLog key in redis.
    """

    def pfadd(self, name, *values):
        """
        Adds the specified elements to the specified HyperLogLog.

        :param name: str     the name of the redis key
        :param values: list of str
        """
        with self.pipe as pipe:
            v_encode = self._valueparse.encode
            values = [v_encode(v) for v in _parse_values(values)]
            return pipe.pfadd(self.redis_key(name), *values)

    def pfcount(self, name):
        """
        Return the approximated cardinality of
        the set observed by the HyperLogLog at key(s).

        :param name: str     the name of the redis key
        """
        with self.pipe as pipe:
            return pipe.pfcount(self.redis_key(name))

    def pfmerge(self, dest, *sources):
        """
        Merge N different HyperLogLogs into a single one.

        :param dest:
        :param sources:
        :return:
        """
        with self.pipe as pipe:
            sources = [self.redis_key(k) for k in sources]
            return pipe.pfmerge(self.redis_key(dest), *sources)
