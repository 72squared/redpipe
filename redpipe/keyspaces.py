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

.. code-block:: python

    redpipe.connect_redis(redis.Redis(
            # connection params go here.
        ), name='user_redis_db')

    class User(redpipe.Hash):
        keyspace = 'user'
        fields = {
            'name': redpipe.TextField,
            'created_at': redpipe.TextField,
        }
        connection = 'user_redis_db'


    user_a = User().hgetall('A')

This Keyspace object exposes all the hash-related redis commands as normal.
Internally, it rewrites the key name to be 'user{A}' for you automatically.
You can pass in a pipeline to the constructor.
No matter what pipeline you pass in, it routes your commands to the
`user_redis_db` that you set up.

There's also support for character encoding and complex data types.
"""
import re
import hashlib
from datetime import (timedelta, datetime)
import typing
from typing import (Dict, Union, Optional, Iterable, Callable, Tuple, Any)
from .pipelines import (autoexec, PipelineInterface)
from .luascripts import lua_restorenx
from .exceptions import InvalidOperation
from .futures import Future
from .fields import (TextField, Field)

__all__ = """
String
HashedString
Set
List
SortedSet
Hash
HyperLogLog
""".split()


def _parse_values(values, extra=None) -> typing.List[Union[str, bytes]]:
    """
    Utility function to flatten out args.

    For internal use only.

    :param values: iterator, or str
    :param extra: list or None
    :return: list
    """
    # returns a single new list combining values and extra
    try:
        iter(values)
        # a string or bytes instance can be iterated, but indicates
        # keys wasn't passed as a list
        if isinstance(values, (str, bytes)):
            values = [values]
        else:
            values = list(values)
    except TypeError:
        values = [values]
    if extra:
        values.extend(extra)
    return values


class Keyspace(object):
    """
    Base class for all keyspace.
    This class should not be used directly.
    """
    __slots__ = ['key', '_pipe']

    keyspace: Optional[str] = None
    connection: Optional[str] = None
    keyparse = TextField
    valueparse = TextField
    keyspace_template = "%s{%s}"

    def __init__(self, pipe: Optional[PipelineInterface] = None):
        """
        Creates a new keyspace.
        Optionally pass in a pipeline object.
        If you pass in a pipeline, all commands to this instance will be
        pipelined.

        :param pipe: optional Pipeline or NestedPipeline
        """
        self._pipe = pipe

    @classmethod
    def redis_key(cls, key: str) -> bytes:
        """
        Get the key we pass to redis.
        If no namespace is declared, it will use the class name.

        :param key: str     the name of the redis key
        :return: str
        """
        keyspace = cls.keyspace
        tpl = cls.keyspace_template
        key = "%s" % key if keyspace is None else tpl % (keyspace, key)
        return cls.keyparse.encode(key)

    @property
    def pipe(self) -> PipelineInterface:
        """
        Get a fresh pipeline() to be used in a `with` block.

        :return: Pipeline or NestedPipeline with autoexec set to true.
        """
        return autoexec(self._pipe, name=self.connection)

    @property
    def super_pipe(self) -> PipelineInterface:
        """
        Creates a mechanism for us to internally bind two different
        operations together in a shared pipeline on the class.
        This will temporarily set self._pipe to be this new pipeline,
        during this context and then when it leaves the context
        reset self._pipe to its original value.

        Example:
            def get_set(self, key, val)
                with self.super_pipe as pipe:
                    res = self.get(key)
                    self.set(key, val)
                    return res

        This will have the effect of using only one network round trip if no
        pipeline was passed to the constructor.

        This method is still considered experimental and we are working out
        the details, so don't use it unless you feel confident you have a
        legitimate use-case for using this.
        """
        orig_pipe = self._pipe

        def exit_handler():
            self._pipe = orig_pipe

        self._pipe = autoexec(orig_pipe, name=self.connection,
                              exit_handler=exit_handler)

        return self._pipe

    def delete(self, *names: str) -> Future:
        """
        Remove the key from redis

        :param names: tuple of strings - The keys to remove from redis.
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.delete(*[self.redis_key(n) for n in names])

    def expire(self, name: str, time: int) -> Future:
        """
        Allow the key to expire after ``time`` seconds.

        :param name: str     the name of the redis key
        :param time: time expressed in seconds.
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.expire(self.redis_key(name), time)

    def exists(self, name) -> Future:
        """
        does the key exist in redis?

        :param name: str the name of the redis key
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.exists(self.redis_key(name))

    def eval(self, script: str, numkeys: int, *keys_and_args) -> Future:
        """
        Run a lua script against the key.
        Doesn't support multi-key lua operations because
        we wouldn't be able to know what argument to namespace.
        Also, redis cluster doesn't really support multi-key operations.

        :param script: str  A lua script targeting the current key.
        :param numkeys: number of keys passed to the script
        :param keys_and_args: list of keys and args passed to script
        :return: Future()
        """
        with self.pipe as pipe:
            args = (a if i >= numkeys else self.redis_key(a) for i, a
                    in enumerate(keys_and_args))
            return pipe.eval(script, numkeys, *args)

    def dump(self, name: str) -> typing.ByteString:
        """
        get a redis RDB-like serialization of the object.

        :param name: str     the name of the redis key
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.dump(self.redis_key(name))

    def restorenx(self,
                  name: str,
                  value: bytes,
                  pttl: int = 0) -> Future:
        """
        Restore serialized dump of a key back into redis

        :param name: str     the name of the redis key
        :param value: redis RDB-like serialization
        :param pttl: milliseconds till key expires
        :return: Future()
        """
        return self.eval(lua_restorenx, 1, name, pttl, value)

    def restore(self,
                name: str,
                value: bytes,
                pttl: int = 0) -> Future[bool]:
        """
        Restore serialized dump of a key back into redis

        :param name: the name of the key
        :param value: the binary representation of the key.
        :param pttl: milliseconds till key expires
        :return:
        """
        with self.pipe as pipe:
            res = pipe.restore(self.redis_key(name), ttl=pttl, value=value)
            f = Future[bool]()

            def cb():
                f.set(self.valueparse.decode(res.result))

            pipe.on_execute(cb)
            return f

    def ttl(self, name: str) -> int:
        """
        get the number of seconds until the key's expiration

        :param name: str     the name of the redis key
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.ttl(self.redis_key(name))

    def persist(self, name: str) -> Future:
        """
        clear any expiration TTL set on the object

        :param name: str     the name of the redis key
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.persist(self.redis_key(name))

    def pexpire(self, name: str, time: Union[int, timedelta]) -> Future:
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

    def pexpireat(self, name: str, when: Union[int, datetime]) -> Future:
        """
        Set an expire flag on key ``name``. ``when`` can be represented
        as an integer representing unix time in milliseconds (unix time * 1000)
        or a Python datetime object.
        """
        with self.pipe as pipe:
            return pipe.pexpireat(self.redis_key(name), when)

    def pttl(self, name: str) -> Future:
        """
        Returns the number of milliseconds until the key ``name`` will expire

        :param name: str    the name of the redis key
        :return: Future int
        """
        with self.pipe as pipe:
            return pipe.pttl(self.redis_key(name))

    def rename(self, src: str, dst: str) -> Future:
        """
        Rename key ``src`` to ``dst``
        """
        with self.pipe as pipe:
            return pipe.rename(self.redis_key(src), self.redis_key(dst))

    def renamenx(self, src: str, dst: str) -> Future:
        "Rename key ``src`` to ``dst`` if ``dst`` doesn't already exist"
        with self.pipe as pipe:
            return pipe.renamenx(self.redis_key(src), self.redis_key(dst))

    def object(self, infotype: str, key: str) -> Future:
        """
        get the key's info stats

        :param infotype: REFCOUNT | ENCODING | IDLETIME
        :param key: str     the name of the redis key
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.object(infotype, self.redis_key(key))

    @classmethod
    def __str__(cls):
        """
        A string representation of the Collection

        :return: str
        """
        return "<%s>" % cls.__name__

    def scan(self,
             cursor: int = 0,
             match: Optional[str] = None,
             count: Optional[int] = None) -> Future[Iterable[Tuple[str, Any]]]:
        """
        Incrementally return lists of key names. Also return a cursor
        indicating the scan position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        f = Future[Iterable[Tuple[str, Any]]]()
        if self.keyspace is None:
            with self.pipe as pipe:
                res = pipe.scan(cursor=cursor, match=match, count=count)

                def cb():
                    f.set((res[0], [self.keyparse.decode(v) for v in res[1]]))

                pipe.on_execute(cb)
                return f

        if match is None:
            match = '*'
        match = "%s{%s}" % (self.keyspace, match)
        pattern = re.compile(r'^%s\{(.*)\}$' % self.keyspace)

        with self.pipe as pipe:

            res = pipe.scan(cursor=cursor, match=match, count=count)

            def cb():
                keys = []
                for k in res[1]:
                    k = self.keyparse.decode(k)
                    m = pattern.match(k)
                    if m:
                        keys.append(m.group(1))

                f.set((res[0], keys))

            pipe.on_execute(cb)
            return f

    def scan_iter(self,
                  match: Optional[str] = None,
                  count: Optional[int] = None) -> Iterable[Tuple[str, Any]]:
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
            cursor, data = self.scan(cursor=int(cursor),
                                     match=match,
                                     count=count)
            for item in data:
                yield item

    def sort(self,
             name: str,
             start: Optional[int] = None,
             num: Optional[int] = None,
             by: Optional[str] = None, get=None,
             desc: bool = False,
             alpha: bool = False,
             store: Optional[str] = None,
             groups: bool = False
             ) -> Future:
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
            res = pipe.sort(
                self.redis_key(name),
                start=start,
                num=num,
                by=self.redis_key(by) if by is not None else None,
                get=self.redis_key(get) if get is not None else None,
                desc=desc,
                alpha=alpha,
                store=self.redis_key(store) if store is not None else None,
                groups=groups)
            if store:
                return res
            f = Future[Iterable[Any]]()

            def cb():
                decode = self.valueparse.decode
                f.set([decode(v) for v in res.result])

            pipe.on_execute(cb)
            return f

    @classmethod
    def _parse_values(cls, values, extra=None):
        return _parse_values(values, extra)


class String(Keyspace):
    """
    Manipulate a String key in Redis.
    """

    def get(self, name: str) -> Future[str]:
        """
        Return the value of the key or None if the key doesn't exist

        :param name: str     the name of the redis key
        :return: Future()
        """
        with self.pipe as pipe:
            f = Future[str]()
            res = pipe.get(self.redis_key(name))

            def cb():
                decode = self.valueparse.decode
                f.set(decode(res.result))

            pipe.on_execute(cb)
            return f

    def mget(self, keys: Union[str, typing.List[str]], *args: str) -> Future:
        """
        Returns a list of values ordered identically to ``keys``
        """
        rkeys = [self.redis_key(k) for k in self._parse_values(keys, args)]
        with self.pipe as pipe:
            f = Future[typing.List[Any]]()
            res = pipe.mget(rkeys)

            def cb():
                decode = self.valueparse.decode
                f.set([None if r is None else decode(r) for r in res.result])

            pipe.on_execute(cb)
            return f

    def set(self,
            name: str,
            value: str,
            ex: Optional[int] = None,
            px: Optional[int] = None,
            nx: bool = False,
            xx: bool = False) -> Future:
        """
        Set the value at key ``name`` to ``value``

        ``ex`` sets an expire flag on key ``name`` for ``ex`` seconds.

        ``px`` sets an expire flag on key ``name`` for ``px`` milliseconds.

        ``nx`` if set to True, set the value at key ``name`` to ``value`` if it
        does not already exist.

        ``xx`` if set to True, set the value at key ``name`` to ``value`` if it
        already exists.

        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.set(self.redis_key(name),
                            self.valueparse.encode(value),
                            ex=ex, px=px, nx=nx, xx=xx)

    def setnx(self, name: str, value: str) -> int:
        """
        Set the value as a string in the key only if the key doesn't exist.

        :param name: str     the name of the redis key
        :param value:
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.setnx(self.redis_key(name),
                              self.valueparse.encode(value))

    def setex(self, name: str, value: str, time: int) -> Future:
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
            return pipe.setex(self.redis_key(name),
                              value=self.valueparse.encode(value),
                              time=time)

    def psetex(self, name: str, value: str, time_ms: int) -> Future:
        """
        Set the value of key ``name`` to ``value`` that expires in ``time_ms``
        milliseconds. ``time_ms`` can be represented by an integer or a Python
        timedelta object
        """
        with self.pipe as pipe:
            return pipe.psetex(self.redis_key(name), time_ms=time_ms,
                               value=self.valueparse.encode(value=value))

    def append(self, name: str, value: str) -> Future:
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
                               self.valueparse.encode(value))

    def strlen(self, name: str) -> Future:
        """
        Return the number of bytes stored in the value of the key

        :param name: str     the name of the redis key
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.strlen(self.redis_key(name))

    def substr(self, name: str, start: int, end: int = -1) -> Future[str]:
        """
        Return a substring of the string at key ``name``. ``start`` and ``end``
        are 0-based integers specifying the portion of the string to return.

        :param name: str     the name of the redis key
        :param start: int
        :param end: int
        :return: Future()
        """
        with self.pipe as pipe:
            f = Future[str]()
            res = pipe.substr(self.redis_key(name), start=start, end=end)

            def cb():
                f.set(self.valueparse.decode(res.result))

            pipe.on_execute(cb)
            return f

    def setrange(self, name: str, offset: int, value: str) -> Future:
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

    def setbit(self, name: str, offset: int, value: str) -> Future:
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

    def getbit(self, name: str, offset: int) -> Future:
        """
        Returns a boolean indicating the value of ``offset`` in key

        :param name: str     the name of the redis key
        :param offset: int
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.getbit(self.redis_key(name), offset)

    def bitcount(self, name, start=None, end=None) -> Future:
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

    def incr(self, name: str, amount: int = 1) -> Future:
        """
        increment the value for key by 1

        :param name: str     the name of the redis key
        :param amount: int
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.incr(self.redis_key(name), amount=amount)

    def incrby(self, name: str, amount: int = 1) -> Future:
        """
        increment the value for key by value: int

        :param name: str     the name of the redis key
        :param amount: int
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.incrby(self.redis_key(name), amount=amount)

    def incrbyfloat(self, name: str, amount: float = 1.0) -> Future:
        """
        increment the value for key by value: float

        :param name: str     the name of the redis key
        :param amount: int
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.incrbyfloat(self.redis_key(name), amount=amount)

    def __getitem__(self, name: str) -> Any:
        """
        magic python method that makes the class behave like a dictionary.

        use to access elements.

        :param name:
        :return:
        """
        return self.get(name)

    def __setitem__(self, name: str, value: str) -> None:
        """
        magic python method that makes the class behave like a dictionary.

        use to set elements.

        :param name:
        :param value:
        :return:
        """
        self.set(name, value)


class HashedStringMeta(type):

    def __new__(mcs, name, bases, d):
        module = 'redpipe.keyspaces'
        if name in ['HashedString'] and d.get('__module__', '') == module:
            return type.__new__(mcs, name, bases, d)

        class Core(Hash):
            keyspace = d.get('keyspace', name)
            connection = d.get('connection', None)
            fields = d.get('fields', {})
            keyparse = d.get('keyparse', TextField)
            valueparse = d.get('valueparse', TextField)
            memberparse = d.get('memberparse', TextField)
            keyspace_template = d.get('keyspace_template', '%s{%s}')

        d['_core'] = Core

        return type.__new__(mcs, name, bases, d)


class HashedString(metaclass=HashedStringMeta):
    _core: Callable
    shard_count = 64

    @classmethod
    def core(cls, pipe: Optional[PipelineInterface] = None):
        return cls._core(pipe)  # typing: ignore

    @classmethod
    def shard(cls, key: str):
        key = "%s" % key
        keyhash = hashlib.md5(key.encode('utf-8')).hexdigest()
        return int(keyhash, 16) % cls.shard_count

    @classmethod
    def _parse_values(cls, values, extra=None):
        return _parse_values(values, extra)

    def __init__(self, pipe: Optional[PipelineInterface] = None):
        """
        Creates a new keyspace.
        Optionally pass in a pipeline object.
        If you pass in a pipeline, all commands to this instance will be
        pipelined.

        :param pipe: optional Pipeline or NestedPipeline
        """
        self._pipe = pipe

    @property
    def pipe(self) -> PipelineInterface:
        """
        Get a fresh pipeline() to be used in a `with` block.

        :return: Pipeline or NestedPipeline with autoexec set to true.
        """
        return autoexec(self._pipe)

    def get(self, key: str) -> Future:
        """
        Return the value of the key or None if the key doesn't exist

        :param key: str     the name of the redis key
        :return: Future()
        """
        with self.pipe as pipe:
            return self.core(pipe=pipe).hget(self.shard(key), key)

    def delete(self, key: str, *args) -> Future[int]:
        keys = self._parse_values(key, args)
        response = Future[int]()
        with self.pipe as pipe:
            core = self.core(pipe=pipe)
            tracking = []
            for k in keys:
                tracking.append(core.hdel(self.shard(k), k))

            def cb():
                response.set(sum(tracking))

            pipe.on_execute(cb)
            return response

    def set(self, name: str, value: str, nx: bool = False) -> Future:
        """
        Set the value at key ``name`` to ``value``

        ``nx`` if set to True, set the value at key ``name`` to ``value`` if it
        does not already exist.

        :return: Future()
        """
        with self.pipe as pipe:
            core = self.core(pipe=pipe)
            method = core.hsetnx if nx else core.hset
            return method(self.shard(name), name, value)

    def setnx(self, name, value) -> Future:
        """
        Set the value as a string in the key only if the key doesn't exist.

        :param name: str     the name of the redis key
        :param value:
        :return: Future()
        """
        with self.pipe as pipe:
            return self.core(pipe=pipe).hsetnx(self.shard(name), name, value)

    def strlen(self, name: str) -> Future:
        """
        Return the number of bytes stored in the value of the key

        :param name: str     the name of the redis key
        :return: Future()
        """
        with self.pipe as pipe:
            return self.core(pipe=pipe).hstrlen(self.shard(name), name)

    def incr(self, name: str, amount: int = 1) -> Future:
        """
        increment the value for key by 1

        :param name: str     the name of the redis key
        :param amount: int
        :return: Future()
        """
        with self.pipe as pipe:
            return self.core(pipe=pipe).hincrby(
                self.shard(name), name, amount=amount)

    def incrby(self, name: str, amount: int = 1) -> Future:
        """
        increment the value for key by value: int

        :param name: str     the name of the redis key
        :param amount: int
        :return: Future()
        """
        with self.pipe as pipe:
            return self.core(pipe=pipe).hincrby(
                self.shard(name), name, amount=amount)

    def incrbyfloat(self, name: str, amount: float = 1.0) -> Future:
        """
        increment the value for key by value: float

        :param name: str     the name of the redis key
        :param amount: int
        :return: Future()
        """
        with self.pipe as pipe:
            return self.core(pipe=pipe).hincrbyfloat(
                self.shard(name), name, amount=amount)

    def __getitem__(self, name: str) -> Future:
        """
        magic python method that makes the class behave like a dictionary.

        use to access elements.

        :param name:
        :return:
        """
        return self.get(name)

    def __setitem__(self, name: str, value: str) -> None:
        """
        magic python method that makes the class behave like a dictionary.

        use to set elements.

        :param name:
        :param value:
        :return:
        """
        self.set(name, value)

    def mget(self, keys: Union[str, typing.List[str]], *args: str) -> Future:
        """
        Returns a list of values ordered identically to ``keys``
        """
        with self.pipe as pipe:
            f = Future[Any]()
            core = self.core(pipe=pipe)
            keys = [k for k in self._parse_values(keys, args)]
            mapping = {k: core.hget(self.shard(k), k) for k in keys}

            def cb():
                f.set([mapping[k] for k in keys])

            pipe.on_execute(cb)

            return f

    def scan_iter(self, match=None, count=None) -> Iterable:
        """
        Make an iterator using the hscan command so that the client doesn't
        need to remember the cursor position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        core = self.core()
        for i in range(0, self.shard_count - 1):
            cursor = 0
            while True:
                res = core.hscan(i, cursor=cursor, match=match, count=count)

                cursor, elements = res
                if elements:
                    for k, v in elements.items():
                        yield k, v

                if cursor == 0:
                    break


class Set(Keyspace):
    """
    Manipulate a Set key in redis.
    """

    def sdiff(self,
              keys: Union[str, typing.List[str]],
              *args: str) -> Future[typing.List[Any]]:
        """
        Return the difference of sets specified by ``keys``

        :param keys: list
        :param args: tuple
        :return: Future()
        """
        rkeys = [self.redis_key(k) for k in self._parse_values(keys, args)]

        with self.pipe as pipe:
            res = pipe.sdiff(*rkeys)
            f = Future[Any]()

            def cb():
                f.set({self.valueparse.decode(v) for v in res.result})

            pipe.on_execute(cb)
            return f

    def sdiffstore(self,
                   dest: str,
                   *keys: Union[str, typing.List[str]]) -> Future:
        """
        Store the difference of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.
        """
        rkeys = (self.redis_key(k) for k in self._parse_values(keys))

        with self.pipe as pipe:
            return pipe.sdiffstore(self.redis_key(dest), *rkeys)

    def sinter(self, keys, *args) -> Future[typing.List[str]]:
        """
        Return the intersection of sets specified by ``keys``

        :param keys: list or str
        :param args: tuple
        :return: Future
        """

        keys = [self.redis_key(k) for k in self._parse_values(keys, args)]
        with self.pipe as pipe:
            res = pipe.sinter(*keys)
            f = Future[typing.List[str]]()

            def cb():
                f.set({self.valueparse.decode(v) for v in res.result})

            pipe.on_execute(cb)
            return f

    def sinterstore(self, dest: str,
                    keys: Union[str, typing.List[str]],
                    *args: str) -> Future:
        """
        Store the intersection of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.
        """
        rkeys = [self.redis_key(k) for k in self._parse_values(keys, args)]
        with self.pipe as pipe:
            return pipe.sinterstore(self.redis_key(dest), rkeys)

    def sunion(self,
               keys: Union[str, typing.List[str]],
               *args: str) -> Future[typing.List[str]]:
        """
        Return the union of sets specified by ``keys``

        :param keys: list or str
        :param args: tuple
        :return: Future()
        """
        rkeys = [self.redis_key(k) for k in self._parse_values(keys, args)]
        with self.pipe as pipe:
            res = pipe.sunion(*rkeys)
            f = Future[typing.List[str]]()

            def cb():
                f.set({self.valueparse.decode(v) for v in res.result})

            pipe.on_execute(cb)
            return f

    def sunionstore(self,
                    dest: str,
                    keys: Union[str, typing.List[str]],
                    *args: str) -> Future:
        """
        Store the union of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of members in the new set.
        """
        rkeys = [self.redis_key(k) for k in self._parse_values(keys, args)]
        with self.pipe as pipe:
            return pipe.sunionstore(self.redis_key(dest), *rkeys)

    def sadd(self,
             name: str,
             values: Union[str, typing.List[str]],
             *args: str) -> Future:
        """
        Add the specified members to the Set.

        :param name: str     the name of the redis key
        :param values: a list of values or a simple value.
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.sadd(self.redis_key(name),
                             *[self.valueparse.encode(v) for v in
                               self._parse_values(values, args)])

    def srem(self,
             name: str,
             *values: Union[str, typing.List[str]]) -> Future:
        """
        Remove the values from the Set if they are present.

        :param name: str     the name of the redis key
        :param values: a list of values or a simple value.
        :return: Future()
        """
        with self.pipe as pipe:
            v_encode = self.valueparse.encode
            return pipe.srem(
                self.redis_key(name),
                *[v_encode(v) for v in self._parse_values(values)])

    def spop(self, name: str) -> Future[typing.List[Any]]:
        """
        Remove and return (pop) a random element from the Set.

        :param name: str     the name of the redis key
        :return: Future()
        """
        with self.pipe as pipe:
            f = Future[typing.List[Any]]()
            res = pipe.spop(self.redis_key(name))

            def cb():
                f.set(self.valueparse.decode(res.result))

            pipe.on_execute(cb)
            return f

    def smembers(self, name: str) -> Future[typing.List[Any]]:
        """
        get the set of all members for key

        :param name: str     the name of the redis key
        :return:
        """
        with self.pipe as pipe:
            f = Future[typing.List[Any]]()
            res = pipe.smembers(self.redis_key(name))

            def cb():
                f.set({self.valueparse.decode(v) for v in res.result})

            pipe.on_execute(cb)
            return f

    def scard(self, name: str) -> Future:
        """
        How many items in the set?

        :param name: str     the name of the redis key
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.scard(self.redis_key(name))

    def sismember(self, name: str, value: str) -> Future:
        """
        Is the provided value is in the ``Set``?

        :param name: str     the name of the redis key
        :param value: str
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.sismember(self.redis_key(name),
                                  self.valueparse.encode(value))

    def srandmember(self,
                    name: str,
                    number: Optional[int] = None
                    ) -> Future[Any]:
        """
        Return a random member of the set.

        :param name: str     the name of the redis key
        :param number: optional int
        :return: Future()
        """
        with self.pipe as pipe:
            f = Future[Any]()
            res = pipe.srandmember(self.redis_key(name), number=number)

            def cb():
                if number is None:
                    f.set(self.valueparse.decode(res.result))
                else:
                    f.set([self.valueparse.decode(v) for v in res.result])

            pipe.on_execute(cb)
            return f

    def sscan(self,
              name: str,
              cursor: int = 0,
              match: Optional[str] = None,
              count: Optional[int] = None
              ) -> Future[Tuple[int, typing.List[Any]]]:
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
            f = Future[Tuple[int, typing.List[Any]]]()
            res = pipe.sscan(self.redis_key(name), cursor=cursor,
                             match=match, count=count)

            def cb():
                f.set((res[0], [self.valueparse.decode(v) for v in res[1]]))

            pipe.on_execute(cb)
            return f

    def sscan_iter(self,
                   name: str,
                   match: Optional[str] = None,
                   count: Optional[int] = None) -> Iterable:
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
            cursor, data = self.sscan(name, cursor=int(cursor),
                                      match=match, count=count)
            for item in data:
                yield item


class List(Keyspace):
    """
    Manipulate a List key in redis
    """

    def blpop(self,
              keys: Union[str, typing.List[str]],
              timeout: int = 0) -> Future[Optional[Tuple[str, Any]]]:
        """
        LPOP a value off of the first non-empty list
        named in the ``keys`` list.

        If none of the lists in ``keys`` has a value to LPOP, then block
        for ``timeout`` seconds, or until a value gets pushed on to one
        of the lists.

        If timeout is 0, then block indefinitely.
        """
        kvpairs = {self.redis_key(k): k for k in self._parse_values(keys)}
        with self.pipe as pipe:
            f = Future[Optional[Tuple[str, Any]]]()
            res = pipe.blpop(kvpairs.keys(), timeout=timeout)

            def cb():
                if res.result:
                    k = kvpairs[res.result[0]]
                    v = self.valueparse.decode(res.result[1])

                    f.set((k, v))
                else:
                    f.set(res.result)

            pipe.on_execute(cb)
            return f

    def brpop(self,
              keys: Union[str, typing.List[str]],
              timeout: int = 0) -> Future[Optional[Tuple[str, Any]]]:
        """
        RPOP a value off of the first non-empty list
        named in the ``keys`` list.

        If none of the lists in ``keys`` has a value to LPOP, then block
        for ``timeout`` seconds, or until a value gets pushed on to one
        of the lists.

        If timeout is 0, then block indefinitely.
        """
        kvpairs = {self.redis_key(k): k for k in self._parse_values(keys)}
        with self.pipe as pipe:
            f = Future[Optional[Tuple[str, Any]]]()
            res = pipe.brpop(kvpairs.keys(), timeout=timeout)

            def cb():
                if res.result:
                    k = kvpairs[res.result[0]]
                    v = self.valueparse.decode(res.result[1])

                    f.set((k, v))
                else:
                    f.set(res.result)

            pipe.on_execute(cb)
            return f

    def brpoplpush(self,
                   src: str,
                   dst: str,
                   timeout: int = 0
                   ) -> Future[Optional[Tuple[str, Any]]]:
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
            f = Future[Optional[Tuple[str, Any]]]()

            def cb():
                f.set(self.valueparse.decode(res.result))

            pipe.on_execute(cb)
            return f

    def llen(self, name: str) -> Future[int]:
        """
        Returns the length of the list.

        :param name: str     the name of the redis key
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.llen(self.redis_key(name))

    def lrange(self,
               name: str,
               start: int,
               stop: int
               ) -> Future[typing.List[Any]]:
        """
        Returns a range of items.

        :param name: str     the name of the redis key
        :param start: integer representing the start index of the range
        :param stop: integer representing the size of the list.
        :return: Future()
        """
        with self.pipe as pipe:
            f = Future[typing.List[Any]]()
            res = pipe.lrange(self.redis_key(name), start, stop)

            def cb():
                f.set([self.valueparse.decode(v) for v in res.result])

            pipe.on_execute(cb)
            return f

    def lpush(self, name: str, *values: str) -> Future[int]:
        """
        Push the value into the list from the *left* side

        :param name: str     the name of the redis key
        :param values: a list of values or single value to push
        :return: Future()
        """
        with self.pipe as pipe:
            v_encode = self.valueparse.encode
            return pipe.lpush(
                self.redis_key(name),
                *[v_encode(v) for v in self._parse_values(values)])

    def rpush(self, name: str, *values: str) -> Future:
        """
        Push the value into the list from the *right* side

        :param name: str     the name of the redis key
        :param values: a list of values or single value to push
        :return: Future()
        """
        with self.pipe as pipe:
            v_encode = self.valueparse.encode
            return pipe.rpush(
                self.redis_key(name),
                *[v_encode(v) for v in self._parse_values(values)])

    def lpop(self, name: str) -> Future[Any]:
        """
        Pop the first object from the left.

        :param name: str     the name of the redis key
        :return: Future()

        """
        with self.pipe as pipe:
            f = Future[Any]()
            res = pipe.lpop(self.redis_key(name))

            def cb():
                f.set(self.valueparse.decode(res.result))

            pipe.on_execute(cb)
            return f

    def rpop(self, name: str) -> Future[Any]:
        """
        Pop the first object from the right.

        :param name: str     the name of the redis key
        :return: the popped value.
        """
        with self.pipe as pipe:
            f = Future[Any]()
            res = pipe.rpop(self.redis_key(name))

            def cb():
                f.set(self.valueparse.decode(res.result))

            pipe.on_execute(cb)
            return f

    def rpoplpush(self, src: str, dst: str) -> Future[Any]:
        """
        RPOP a value off of the ``src`` list and atomically LPUSH it
        on to the ``dst`` list.  Returns the value.
        """
        with self.pipe as pipe:
            f = Future[Any]()
            res = pipe.rpoplpush(self.redis_key(src), self.redis_key(dst))

            def cb():
                f.set(self.valueparse.decode(res.result))

            pipe.on_execute(cb)
            return f

    def lrem(self, name: str, value: str, num: int = 1) -> Future[int]:
        """
        Remove first occurrence of value.

        Can't use redis-py interface. It's inconstistent between
        redis.Redis and redis.StrictRedis in terms of the kwargs.
        Better to use the underlying execute_command instead.

        :param name: str     the name of the redis key
        :param num:
        :param value:
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.execute_command('LREM', self.redis_key(name),
                                        num, self.valueparse.encode(value))

    def ltrim(self, name: str, start: int, end: int) -> Future:
        """
        Trim the list from start to end.

        :param name: str     the name of the redis key
        :param start:
        :param end:
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.ltrim(self.redis_key(name), start, end)

    def lindex(self, name: str, index: int) -> Future[Any]:
        """
        Return the value at the index *idx*

        :param name: str     the name of the redis key
        :param index: the index to fetch the value.
        :return: Future()
        """
        with self.pipe as pipe:
            f = Future[Any]()
            res = pipe.lindex(self.redis_key(name), index)

            def cb():
                f.set(self.valueparse.decode(res.result))

            pipe.on_execute(cb)
            return f

    def lset(self, name: str, index: int, value: str) -> Future:
        """
        Set the value in the list at index *idx*

        :param name: str     the name of the redis key
        :param index:
        :param value:
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.lset(self.redis_key(name), index,
                             self.valueparse.encode(value))


class SortedSet(Keyspace):
    """
    Manipulate a SortedSet key in redis.
    """

    def zadd(self,
             name: str,
             members: Union[str, typing.List[str]],
             score: float = 1.0,
             nx: bool = False,
             xx: bool = False,
             ch: bool = False,
             incr: bool = False) -> Future:
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
        _args: typing.List[Union[bytes, str]]
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
                _args += [str(score), self.valueparse.encode(member)]
        elif isinstance(members, str):
            _args += [str(score), self.valueparse.encode(members)]

        if nx and xx:
            raise InvalidOperation('cannot specify nx and xx at the same time')
        with self.pipe as pipe:
            return pipe.execute_command('ZADD', self.redis_key(name), *_args)

    def zrem(self, name: str, *values: str) -> Future:
        """
        Remove the values from the SortedSet

        :param name: str     the name of the redis key
        :param values:
        :return: True if **at least one** value is successfully
                 removed, False otherwise
        """
        with self.pipe as pipe:
            v_encode = self.valueparse.encode
            return pipe.zrem(
                self.redis_key(name),
                *[v_encode(v) for v in self._parse_values(values)])

    def zincrby(self, name: str, value: Any, amount: float = 1.0) -> Future:
        """
        Increment the score of the item by `value`

        :param name: str     the name of the redis key
        :param value:
        :param amount:
        :return:
        """
        with self.pipe as pipe:
            return pipe.zincrby(self.redis_key(name),
                                value=self.valueparse.encode(value),
                                amount=amount)

    def zrevrank(self, name: str, value: str):
        """
        Returns the ranking in reverse order for the member

        :param name: str     the name of the redis key
        :param value: str
        """
        with self.pipe as pipe:
            return pipe.zrevrank(self.redis_key(name),
                                 self.valueparse.encode(value))

    def zrange(self,
               name: str,
               start: int,
               end: int,
               desc: bool = False,
               withscores: bool = False,
               score_cast_func: Callable = float
               ) -> Future[typing.List[Any]]:
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
            f = Future[typing.List[Any]]()
            res = pipe.zrange(
                self.redis_key(name), start, end, desc=desc,
                withscores=withscores, score_cast_func=score_cast_func)

            def cb():
                if withscores:
                    f.set([(self.valueparse.decode(v), s) for v, s in
                           res.result])
                else:
                    f.set([self.valueparse.decode(v) for v in res.result])

            pipe.on_execute(cb)
            return f

    def zrevrange(self,
                  name: str,
                  start: int,
                  end: int,
                  withscores: bool = False,
                  score_cast_func: Callable = float
                  ) -> Future[typing.List[Any]]:
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
            f = Future[typing.List[Any]]()
            res = pipe.zrevrange(self.redis_key(name), start, end,
                                 withscores=withscores,
                                 score_cast_func=score_cast_func)

            def cb():
                if withscores:
                    f.set([(self.valueparse.decode(v), s) for v, s in
                           res.result])
                else:
                    f.set([self.valueparse.decode(v) for v in res.result])

            pipe.on_execute(cb)
            return f

    # noinspection PyShadowingBuiltins
    def zrangebyscore(self,
                      name: str,
                      min: float,
                      max: float,
                      start: Optional[int] = None,
                      num: Optional[int] = None,
                      withscores: bool = False,
                      score_cast_func: Callable = float
                      ) -> Future[typing.List[Any]]:
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
            f = Future[typing.List[Any]]()
            res = pipe.zrangebyscore(self.redis_key(name), min, max,
                                     start=start, num=num,
                                     withscores=withscores,
                                     score_cast_func=score_cast_func)

            def cb():
                if withscores:
                    f.set([(self.valueparse.decode(v), s) for v, s in
                           res.result])
                else:
                    f.set([self.valueparse.decode(v) for v in res.result])

            pipe.on_execute(cb)
            return f

    # noinspection PyShadowingBuiltins
    def zrevrangebyscore(self,
                         name: str,
                         max: float,
                         min: float,
                         start: int = None,
                         num: int = None,
                         withscores: bool = False,
                         score_cast_func: Callable = float
                         ) -> Future[typing.List[Any]]:
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
            f = Future[typing.List[Any]]()
            res = pipe.zrevrangebyscore(self.redis_key(name), max, min,
                                        start=start, num=num,
                                        withscores=withscores,
                                        score_cast_func=score_cast_func)

            def cb():
                if withscores:
                    f.set([(self.valueparse.decode(v), s) for v, s in
                           res.result])
                else:
                    f.set([self.valueparse.decode(v) for v in res.result])

            pipe.on_execute(cb)
            return f

    def zcard(self, name: str) -> Future[int]:
        """
        Returns the cardinality of the SortedSet.

        :param name: str     the name of the redis key
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.zcard(self.redis_key(name))

    # noinspection PyShadowingBuiltins
    def zcount(self, name: str, min: float, max: float) -> Future[int]:
        """
        Returns the number of elements in the sorted set at key ``name`` with
        a score between ``min`` and ``max``.

        :param name: str
        :param min: float
        :param max: float
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.zcount(self.redis_key(name), min, max)

    def zscore(self, name: str, value: Any) -> Future[float]:
        """
        Return the score of an element

        :param name: str     the name of the redis key
        :param value: the element in the sorted set key
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.zscore(self.redis_key(name),
                               self.valueparse.encode(value))

    # noinspection PyShadowingBuiltins
    def zremrangebyrank(self,
                        name: str,
                        min: float,
                        max: float
                        ) -> Future[int]:
        """
        Remove a range of element between the rank ``start`` and
        ``stop`` both included.

        :param name: str     the name of the redis key
        :param min:
        :param max:
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.zremrangebyrank(self.redis_key(name), min, max)

    # noinspection PyShadowingBuiltins
    def zremrangebyscore(self,
                         name: str,
                         min: int,
                         max: int
                         ) -> Future[typing.List[Any]]:
        """
        Remove a range of element by between score ``min_value`` and
        ``max_value`` both included.

        :param name: str     the name of the redis key
        :param min:
        :param max:
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.zremrangebyscore(self.redis_key(name), min, max)

    def zrank(self, name: str, value: str) -> Future[int]:
        """
        Returns the rank of the element.

        :param name: str     the name of the redis key
        :param value: the element in the sorted set
        """
        with self.pipe as pipe:
            return pipe.zrank(self.redis_key(name),
                              self.valueparse.encode(value))

    # noinspection PyShadowingBuiltins
    def zlexcount(self, name: str, min: float, max: float) -> Future[int]:
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
    def zrangebylex(self,
                    name: str,
                    min: float,
                    max: float,
                    start: int = None,
                    num: int = None
                    ) -> Future[typing.List[Any]]:
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
            f = Future[typing.List[Any]]()
            res = pipe.zrangebylex(self.redis_key(name), min, max,
                                   start=start, num=num)

            def cb():
                f.set([self.valueparse.decode(v) for v in res])

            pipe.on_execute(cb)
            return f

    # noinspection PyShadowingBuiltins
    def zrevrangebylex(self,
                       name: str,
                       max: float,
                       min: float,
                       start: int = None,
                       num: int = None
                       ) -> Future[typing.List[Any]]:
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
            f = Future[typing.List[Any]]()
            res = pipe.zrevrangebylex(self.redis_key(name), max, min,
                                      start=start, num=num)

            def cb():
                f.set([self.valueparse.decode(v) for v in res])

            pipe.on_execute(cb)
            return f

    # noinspection PyShadowingBuiltins
    def zremrangebylex(self, name: str, min: float, max: float) -> Future[int]:
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

    def zunionstore(self, dest: str,
                    keys: typing.List[str],
                    aggregate: Optional[str] = None) -> Future[int]:
        """
        Union multiple sorted sets specified by ``keys`` into
        a new sorted set, ``dest``. Scores in the destination will be
        aggregated based on the ``aggregate``, MIN, MAX,
        or SUM if none is provided.
        """
        with self.pipe as pipe:
            return pipe.zunionstore(self.redis_key(dest),
                                    [self.redis_key(k) for k in keys],
                                    aggregate=aggregate)

    def zscan(self,
              name: str,
              cursor: int = 0,
              match: Optional[str] = None,
              count: Optional[int] = None,
              score_cast_func: Callable = float
              ) -> Future[Tuple[int, typing.List[Tuple[str, Any]]]]:
        """
        Incrementally return lists of elements in a sorted set. Also return a
        cursor indicating the scan position.

        ``match`` allows for filtering the members by pattern

        ``count`` allows for hint the minimum number of returns

        ``score_cast_func`` a callable used to cast the score return value
        """
        with self.pipe as pipe:
            f = Future[Tuple[int, typing.List[Tuple[str, Any]]]]()
            res = pipe.zscan(self.redis_key(name), cursor=cursor,
                             match=match, count=count,
                             score_cast_func=score_cast_func)

            def cb():
                f.set((res[0], [(self.valueparse.decode(k), v)
                                for k, v in res[1]]))

            pipe.on_execute(cb)
            return f

    def zscan_iter(self,
                   name: str,
                   match: Optional[str] = None,
                   count: Optional[int] = None,
                   score_cast_func: Callable = float
                   ) -> Iterable[Tuple[str, Any]]:
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
            cursor, data = self.zscan(name, cursor=int(cursor), match=match,
                                      count=count,
                                      score_cast_func=score_cast_func)
            for item in data:
                yield item


class Hash(Keyspace):
    """
    Manipulate a Hash key in Redis.
    """

    fields: Dict[str, Field] = {}

    memberparse = TextField

    @classmethod
    def _value_encode(cls, member, value):
        """
        Internal method used to encode values into the hash.

        :param member: str
        :param value: multi
        :return: bytes
        """
        try:
            field_validator = cls.fields[member]
        except KeyError:
            return cls.valueparse.encode(value)

        return field_validator.encode(value)

    @classmethod
    def _value_decode(cls, member, value):
        """
        Internal method used to decode values from redis hash

        :param member: str
        :param value: bytes
        :return: multi
        """
        if value is None:
            return None
        try:
            field_validator = cls.fields[member]
        except KeyError:
            return cls.valueparse.decode(value)

        return field_validator.decode(value)

    def hlen(self, name: str) -> Future[int]:
        """
        Returns the number of elements in the Hash.

        :param name: str     the name of the redis key
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.hlen(self.redis_key(name))

    def hstrlen(self, name: str, key: str) -> Future:
        """
        Return the number of bytes stored in the value of ``key``
        within hash ``name``
        """
        with self.pipe as pipe:
            return pipe.hstrlen(self.redis_key(name), key)

    def hset(self, name: str, key: str, value: Any) -> Future[int]:
        """
        Set ``member`` in the Hash at ``value``.

        :param name: str     the name of the redis key
        :param value:
        :param key: the member of the hash key
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.hset(self.redis_key(name),
                             self.memberparse.encode(key),
                             self._value_encode(key, value))

    def hsetnx(self, name: str, key: str, value: Any) -> Future[int]:
        """
        Set ``member`` in the Hash at ``value``.

        :param name: str     the name of the redis key
        :param value:
        :param key:
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.hsetnx(self.redis_key(name),
                               self.memberparse.encode(key),
                               self._value_encode(key, value))

    def hdel(self, name: str, *keys) -> Future[int]:
        """
        Delete one or more hash field.

        :param name: str     the name of the redis key
        :param keys: on or more members to remove from the key.
        :return: Future()
        """
        with self.pipe as pipe:
            m_encode = self.memberparse.encode
            return pipe.hdel(
                self.redis_key(name),
                *[m_encode(m) for m in self._parse_values(keys)])

    def hkeys(self, name: str) -> Future[typing.List[str]]:
        """
        Returns all fields name in the Hash.

        :param name: str the name of the redis key
        :return: Future
        """
        with self.pipe as pipe:
            f = Future[typing.List[str]]()
            res = pipe.hkeys(self.redis_key(name))

            def cb():
                m_decode = self.memberparse.decode
                f.set([m_decode(v) for v in res.result])

            pipe.on_execute(cb)
            return f

    def hgetall(self, name: str) -> Future[Dict[str, Any]]:
        """
        Returns all the fields and values in the Hash.

        :param name: str     the name of the redis key
        :return: Future()
        """
        with self.pipe as pipe:
            f = Future[Dict[str, Any]]()
            res = pipe.hgetall(self.redis_key(name))

            def cb():
                data = {}
                m_decode = self.memberparse.decode
                v_decode = self._value_decode
                for k, v in res.result.items():
                    k = m_decode(k)
                    v = v_decode(k, v)
                    data[k] = v
                f.set(data)

            pipe.on_execute(cb)
            return f

    def hvals(self, name: str) -> Future[typing.List[Any]]:
        """
        Returns all the values in the Hash
        Unfortunately we can't type cast these fields.
        it is a useless call anyway imho.

        :param name: str     the name of the redis key
        :return: Future()
        """
        with self.pipe as pipe:
            f = Future[typing.List[Any]]()
            res = pipe.hvals(self.redis_key(name))

            def cb():
                f.set([self.valueparse.decode(v) for v in res.result])

            pipe.on_execute(cb)
            return f

    def hget(self, name: str, key: str) -> Future[Any]:
        """
        Returns the value stored in the field, None if the field doesn't exist.

        :param name: str     the name of the redis key
        :param key: the member of the hash
        :return: Future()
        """
        with self.pipe as pipe:
            f = Future[Any]()
            res = pipe.hget(self.redis_key(name),
                            self.memberparse.encode(key))

            def cb():
                f.set(self._value_decode(key, res.result))

            pipe.on_execute(cb)
            return f

    def hexists(self, name: str, key: str) -> Future[bool]:
        """
        Returns ``True`` if the field exists, ``False`` otherwise.

        :param name: str     the name of the redis key
        :param key: the member of the hash
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.hexists(self.redis_key(name),
                                self.memberparse.encode(key))

    def hincrby(self, name: str, key: str, amount: int = 1) -> Future[int]:
        """
        Increment the value of the field.

        :param name: str     the name of the redis key
        :param key: str
        :param amount: int
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.hincrby(self.redis_key(name),
                                self.memberparse.encode(key),
                                amount)

    def hincrbyfloat(self,
                     name: str,
                     key: str,
                     amount: float = 1.0
                     ) -> Future[float]:
        """
        Increment the value of the field.

        :param name: str     the name of the redis key
        :param key: the name of the emement in the hash
        :param amount: float
        :return: Future()
        """
        with self.pipe as pipe:
            return pipe.hincrbyfloat(self.redis_key(name),
                                     self.memberparse.encode(key),
                                     amount)

    def hmget(self,
              name: str,
              keys: typing.List[str],
              *args) -> Future[typing.List[Any]]:
        """
        Returns the values stored in the fields.

        :param name: str     the name of the redis key
        :param keys:
        :return: Future()
        """
        member_encode = self.memberparse.encode
        with self.pipe as pipe:
            f = Future[typing.List[Any]]()
            keys = [k for k in self._parse_values(keys, args)]
            res = pipe.hmget(self.redis_key(name),
                             [member_encode(k) for k in keys])

            def cb():
                f.set([self._value_decode(keys[i], v)
                       for i, v in enumerate(res.result)])

            pipe.on_execute(cb)
            return f

    def hmset(self, name: str, mapping: Dict[str, Any]) -> Future[None]:
        """
        Sets or updates the fields with their corresponding values.

        :param name: str     the name of the redis key
        :param mapping: a dict with keys and values
        :return: Future()
        """
        with self.pipe as pipe:
            m_encode = self.memberparse.encode
            return pipe.hmset(self.redis_key(name),
                              {m_encode(k): self._value_encode(k, v)
                               for k, v in mapping.items()})

    def hscan(self,
              name: str,
              cursor: int = 0,
              match: Optional[str] = None,
              count: Optional[int] = None
              ) -> Future[typing.Tuple[int, Dict[str, Any]]]:
        """
        Incrementally return key/value slices in a hash. Also return a cursor
        indicating the scan position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        with self.pipe as pipe:
            f = Future[typing.Tuple[int, Dict[str, Any]]]()
            res = pipe.hscan(self.redis_key(name), cursor=cursor,
                             match=match, count=count)

            def cb():
                data = {}
                m_decode = self.memberparse.decode
                for k, v in res[1].items():
                    k = m_decode(k)
                    v = self._value_decode(k, v)
                    data[k] = v

                f.set((res[0], data))

            pipe.on_execute(cb)
            return f

    def hscan_iter(self, name: str,
                   match: Optional[str] = None,
                   count: Optional[int] = None
                   ) -> Iterable[Tuple[str, Any]]:
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
            cursor, data = self.hscan(name, cursor=int(cursor),
                                      match=match, count=count)
            for item in data.items():
                yield item


class HyperLogLog(Keyspace):
    """
    Manipulate a HyperLogLog key in redis.
    """

    def pfadd(self, name: str, *values: str) -> Future[int]:
        """
        Adds the specified elements to the specified HyperLogLog.

        :param name: str     the name of the redis key
        :param values: list of str
        """
        with self.pipe as pipe:
            v_encode = self.valueparse.encode
            return pipe.pfadd(
                self.redis_key(name),
                *[v_encode(v) for v in self._parse_values(values)])

    def pfcount(self, *sources: str) -> Future[int]:
        """
        Return the approximated cardinality of
        the set observed by the HyperLogLog at key(s).

        Using the execute_command because redis-py-cluster disabled it
        unnecessarily. but you can only send one key at a time in that case,
        or only keys that map to the same keyslot.
        Use at your own risk.

        :param sources: [str]     the names of the redis keys
        """
        with self.pipe as pipe:
            return pipe.execute_command('PFCOUNT',
                                        *[self.redis_key(s) for s in sources])

    def pfmerge(self, dest: str, *sources: str) -> Future[None]:
        """
        Merge N different HyperLogLogs into a single one.

        :param dest:
        :param sources:
        :return:
        """
        with self.pipe as pipe:
            return pipe.pfmerge(self.redis_key(dest),
                                *[self.redis_key(k) for k in sources])
