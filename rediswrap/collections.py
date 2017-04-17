import hashlib
from .context import PipelineContext
from .lua import lua_restorenx, lua_object_info
from .exceptions import InvalidOperation
from .result import DeferredResult
from .compat import long

__all__ = """
String
Set
List
SortedSet
Hash
ShardedHash
""".split()


class Collection(object):
    """
    Base class for all collections. This class should not be used directly.
    """
    __slots__ = ['key', 'namespace', '_pipe']

    def __init__(self, key, pipe=None):
        """
        Pass in the key identifier for your object. This should not
        include the namespace. For example if you pass in 'foo', and
        your class has the namespace of A, when we talk to redis, the
        key will be: `A{foo}`.

        This allows for consistent namespacing and iteration through
        all keys in the namespace.

        :param key: str The name of your key.
        :param pipe: optional Pipeline or PipelineContext
        """
        self.key = key
        self._pipe = pipe

    @property
    def _key(self):
        """
        Get the key we pass to redis.
        If no namespace is declared, it will use the class name.
        :return: str
        """
        try:
            namespace = self._namespace
        except AttributeError:
            namespace = self.__class__.__name__

        return "%s{%s}" % (namespace, self.key)

    @property
    def pipe(self):
        """
        Get a fresh PipelineContext to be used in a `with` block.
        :return: PipelineContext()
        """
        return PipelineContext(self._pipe)

    def delete(self):
        """
        Remove the collection from redis
        > s = Set('test')
        > s.add('1').result
        1
        > s.delete()
        > s.members().result
        set([])
        :return: DeferredResult()
        """
        with self.pipe as pipe:
            return pipe.delete(self._key)

    def expire(self, time):
        """
        Allow the key to expire after ``time`` seconds.

        > with PipelineContext() as pipe:
        >   s = Set("test", pipe=pipe)
        >   s.add("1")
        >   s.set_expire(1)
        >   time.sleep(1)
        > Set("test").members().result
        set([])

        :param time: time expressed in seconds.
        :return: DeferredResult()
        """
        with self.pipe as pipe:
            return pipe.expire(self._key, time)

    def exists(self):
        """
        does the key exist in redis?
        :return: DeferredResult()
        """
        with self.pipe as pipe:
            return pipe.exists(self._key)

    def eval(self, script, *args):
        """
        Run a lua script against the key.
        :param script: str  A lua script targeting the current key.
        :param args: arguments to be passed to redis for the lua script
        :return: DeferredResult()
        """
        with self.pipe as pipe:
            return pipe.eval(script, 1, self.key, *args)

    def dump(self):
        """
        get a redis RDB-like serialization of the object.
        :return: DeferredResult()
        """
        with self.pipe as pipe:
            return pipe.dump(self._key)

    def restore(self, data, pttl=0):
        """

        :param data: redis RDB-like serialization
        :param pttl: how many milliseconds till expiration of the key.
        :return: DeferredResult()
        """
        return self.eval(lua_restorenx, pttl, data)

    def ttl(self):
        """
        get the number of seconds until the key's expiration
        :return: DeferredResult()
        """
        with self.pipe as pipe:
            return pipe.ttl(self._key)

    def persist(self):
        """
        clear any expiration TTL set on the object
        :return: DeferredResult()
        """
        with self.pipe as pipe:
            return pipe.persist(self._key)

    def object(self, subcommand):
        """
        get the key's info stats
        :param subcommand: REFCOUNT | ENCODING | IDLETIME
        :return: DeferredResult()
        """
        return self.eval(lua_object_info, subcommand)

    def __str__(self):
        """
        A string representation of the Collection
        :return: str
        """
        return "<%s:%s>" % (self.__class__.__name__, self.key)


class String(Collection):
    def get(self):
        """
        set the value as a string in the key
        """
        with self.pipe as pipe:
            return pipe.get(self._key)

    def set(self, value):
        """
        set the value as a string in the key
        :param value:
        """
        with self.pipe as pipe:
            return pipe.set(self._key, value)

    def incr(self):
        """
        increment the value for key by 1
        """
        with self.pipe as pipe:
            return pipe.incr(self._key)

    def incrby(self, value=1):
        """
        increment the value for key by value: int
        :param value:
        """
        with self.pipe as pipe:
            return pipe.incrby(self._key, value)

    def incrbyfloat(self, value=1.0):
        """
        increment the value for key by value: float
        :param value:
        """
        with self.pipe as pipe:
            return pipe.incrbyfloat(self._key, value)

    def setnx(self, value):
        """
        Set the value as a string in the key only if the key doesn't exist.
        :param value:
        :return:
        """
        with self.pipe as pipe:
            return pipe.setnx(self._key, value)


def _parse_values(values):
    (_values,) = values if len(values) == 1 else (None,)
    if _values and isinstance(_values, list):
        return _values
    return values


class Set(Collection):
    """
    .. default-domain:: set

    This class represent a Set in redis.
    """

    def sadd(self, *values):
        """
        Add the specified members to the Set.

        :param values: a list of values or a simple value.
        :rtype: integer representing the number of value added to the set.

        > s = Set("test")
        > s.delete()
        > s.add(["1", "2", "3"])
        3
        > s.add(["4"])
        1
        > print s
        <Set 'test' set(['1', '3', '2', '4'])>
        > s.delete()

        """
        with self.pipe as pipe:
            return pipe.sadd(self._key, *_parse_values(values))

    def srem(self, *values):
        """
        Remove the values from the Set if they are present.

        :param values: a list of values or a simple value.
        :rtype: boolean indicating if the values have been removed.

        > s = Set("test")
        > s.add(["1", "2", "3"])
        3
        > s.srem(["1", "3"])
        2
        > s.delete()

        """
        with self.pipe as pipe:
            return pipe.srem(self._key, *_parse_values(values))

    def spop(self):
        """
        Remove and return (pop) a random element from the Set.

        :rtype: String representing the value poped.

        > s = Set("test")
        > s.add("1")
        1
        > s.spop()
        '1'
        > s.members
        set([])

        """
        with self.pipe as pipe:
            return pipe.spop(self._key)

    def smembers(self):
        with self.pipe as pipe:
            return pipe.smembers(self._key)

    members = smembers

    def scard(self):
        """
        Returns the cardinality of the Set.

        :rtype: String containing the cardinality.

        """
        with self.pipe as pipe:
            return pipe.scard(self._key)

    def sismember(self, value):
        """
        Return ``True`` if the provided value is in the ``Set``.
        :param value:

        """
        with self.pipe as pipe:
            return pipe.sismember(self._key, value)

    def srandmember(self):
        """
        Return a random member of the set.

        > s = Set("test")
        > s.add(['a', 'b', 'c'])
        3
        > s.srandmember() # doctest: +ELLIPSIS
        '...'
        > # 'a', 'b' or 'c'
        """
        with self.pipe as pipe:
            return pipe.srandmember(self._key)

    add = sadd
    pop = spop
    remove = srem


class List(Collection):
    """
    This class represent a list object as seen in redis.
    """

    def all(self):
        """
        Returns all items in the list.
        """
        return self.lrange(0, -1)

    members = property(all)

    def llen(self):
        """
        Returns the length of the list.
        """
        with self.pipe as pipe:
            return pipe.llen(self._key)

    def lrange(self, start, stop):
        """
        Returns a range of items.

        :param start: integer representing the start index of the range
        :param stop: integer representing the size of the list.

        > l = List("test")
        > l.push(['a', 'b', 'c', 'd'])
        4L
        > l.lrange(1, 2)
        ['b', 'c']
        > l.delete()

        """
        with self.pipe as pipe:
            return pipe.lrange(self._key, start, stop)

    def lpush(self, *values):
        """
        Push the value into the list from the *left* side

        :param values: a list of values or single value to push
        :rtype: long representing the number of values pushed.

        > l = List("test")
        > l.lpush(['a', 'b'])
        2L
        > l.delete()
        """
        with self.pipe as pipe:
            return pipe.lpush(self._key, *_parse_values(values))

    def rpush(self, *values):
        """
        Push the value into the list from the *right* side

        :param values: a list of values or single value to push
        :rtype: long representing the size of the list.

        > l = List("test")
        > l.lpush(['a', 'b'])
        2L
        > l.rpush(['c', 'd'])
        4L
        > l.members
        ['b', 'a', 'c', 'd']
        > l.delete()
        """
        with self.pipe as pipe:
            return pipe.rpush(self._key, *_parse_values(values))

    def extend(self, iterable):
        """
        Extend list by appending elements from the iterable.

        :param iterable: an iterable objects.
        """
        with self.pipe as pipe:
            for e in iterable:
                pipe.rpush(self._key, *_parse_values(e))

    def lpop(self):
        """
        Pop the first object from the left.

        :return: the popped value.

        """
        with self.pipe as pipe:
            return pipe.lpop(self._key)

    def rpop(self):
        """
        Pop the first object from the right.

        :return: the popped value.
        """
        with self.pipe as pipe:
            return pipe.rpop(self._key)

    def lrem(self, value, num=1):
        """
        Remove first occurrence of value.
        :param num:
        :param value:
        :return: 1 if the value has been removed, 0 otherwise
        if you see an error here, did you use redis.StrictRedis()?
        """
        with self.pipe as pipe:
            return pipe.lrem(self._key, num, value)

    def ltrim(self, start, end):
        """
        Trim the list from start to end.

        :param start:
        :param end:
        :return: None
        """
        with self.pipe as pipe:
            return pipe.ltrim(self._key, start, end)

    def lindex(self, idx):
        """
        Return the value at the index *idx*

        :param idx: the index to fetch the value.
        :return: the value or None if out of range.
        """
        with self.pipe as pipe:
            return pipe.lindex(self._key, idx)

    def lset(self, idx, value=0):
        """
        Set the value in the list at index *idx*

        :param value:
        :param idx:
        :return: True is the operation succeed.

        > l = List('test')
        > l.push(['a', 'b', 'c'])
        3L
        > l.lset(0, 'e')
        True
        > l.members
        ['e', 'b', 'c']
        > l.delete()

        """
        with self.pipe as pipe:
            return pipe.lset(self._key, idx, value)

    # noinspection PyRedeclaration
    remove = lrem
    trim = ltrim
    shift = lpop
    unshift = lpush
    pop = rpop
    push = rpush
    append = rpush


class SortedSet(Collection):
    """
    This class represents a SortedSet in redis.
    Use it if you want to arrange your set in any order.

    """

    @property
    def members(self):
        """
        Returns the members of the set.
        """
        return self.zrange(0, -1)

    @property
    def revmembers(self):
        """
        Returns the members of the set in reverse.
        """
        return self.zrevrange(0, -1)

    def lt(self, v, limit=None, offset=None):
        """
        Returns the list of the members of the set that have scores
        less than v.

        :param v: the score to compare to.
        :param limit: limit the result to *n* elements
        :param offset: Skip the first *n* elements
        """
        if limit is not None and offset is None:
            offset = 0
        return self.zrangebyscore("-inf", "(%f" % v, start=offset, num=limit)

    def le(self, v, limit=None, offset=None):
        """
        Returns the list of the members of the set that have scores
        less than or equal to v.

        :param v: the score to compare to.
        :param limit: limit the result to *n* elements
        :param offset: Skip the first *n* elements

        """
        if limit is not None and offset is None:
            offset = 0
        return self.zrangebyscore("-inf", v, start=offset, num=limit)

    def gt(self, v, limit=None, offset=None, withscores=False):
        """Returns the list of the members of the set that have scores
        greater than v.
        :param withscores:
        :param offset:
        :param limit:
        :param v:
        """
        if limit is not None and offset is None:
            offset = 0
        return self.zrangebyscore(
            "(%f" % v, "+inf",
            start=offset,
            num=limit,
            withscores=withscores)

    def ge(self, v, limit=None, offset=None, withscores=False):
        """Returns the list of the members of the set that have scores
        greater than or equal to v.

        :param withscores:
        :param v: the score to compare to.
        :param limit: limit the result to *n* elements
        :param offset: Skip the first *n* elements

        """
        if limit is not None and offset is None:
            offset = 0
        return self.zrangebyscore(
            "%f" % v, "+inf",
            start=offset,
            num=limit,
            withscores=withscores)

    def between(self, low, high, limit=None, offset=None):
        """
        Returns the list of the members of the set that have scores
        between min and max.

        .. Note::
            The min and max are inclusive when comparing the values.

        :param low: the minimum score to compare to.
        :param high: the maximum score to compare to.
        :param limit: limit the result to *n* elements
        :param offset: Skip the first *n* elements

        > s = SortedSet("foo")
        > s.add('a', 10)
        1
        > s.add('b', 20)
        1
        > s.add('c', 30)
        1
        > s.between(20, 30)
        ['b', 'c']
        > s.delete()
        """
        if limit is not None and offset is None:
            offset = 0
        return self.zrangebyscore(low, high, start=offset, num=limit)

    def zadd(self, members, score=1, nx=False, xx=False, ch=False, incr=False):
        """
        Add members in the set and assign them the score.

        :param members: a list of item or a single item
        :param score: the score the assign to the item(s)

        > s = SortedSet("foo")
        > s.add('a', 10)
        1
        > s.add('b', 20)
        1
        > s.delete()
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
                _args += [score, member]
        else:
            _args += [score, members]

        if nx and xx:
            raise InvalidOperation('cannot specify nx and xx at the same time')
        with self.pipe as pipe:
            return pipe.execute_command('ZADD', self._key, *_args)

    def zrem(self, *values):
        """
        Remove the values from the SortedSet

        :param values:
        :return: True if **at least one** value is successfully
                 removed, False otherwise

        > s = SortedSet('foo')
        > s.add('a', 10)
        1
        > s.zrem('a')
        1
        > s.members
        []
        > s.delete()
        """
        with self.pipe as pipe:
            return pipe.zrem(self._key, *_parse_values(values))

    def zincrby(self, att, value=1):
        """
        Increment the score of the item by ``value``
         > s = SortedSet("foo")
        > s.add('a', 10)
        1
        > s.zincrby("a", 10)
        20.0
        > s.delete()
        :param att: the member to increment
        :param value: the value to add to the current score
        :returns: the new score of the member


        """
        with self.pipe as pipe:
            return pipe.zincrby(self._key, att, value)

    def zrevrank(self, member):
        """
        Returns the ranking in reverse order for the member

        > s = SortedSet("foo")
        > s.add('a', 10)
        1
        > s.add('b', 20)
        1
        > s.revrank('a')
        1
        > s.delete()
        :param member: str
        """
        with self.pipe as pipe:
            return pipe.zrevrank(self._key, member)

    def zrange(self, start, end, **kwargs):
        """
        Returns all the elements including between ``start`` (non included) and
        ``stop`` (included).

        :param kwargs:
        :param end:
        :param start:
        > s = SortedSet("foo")
        > s.add('a', 10)
        1
        > s.add('b', 20)
        1
        > s.add('c', 30)
        1
        > s.zrange(1, 3)
        ['b', 'c']
        > s.zrange(1, 3, withscores=True)
        [('b', 20.0), ('c', 30.0)]
        > s.delete()
        """
        with self.pipe as pipe:
            return pipe.zrange(self._key, start, end, **kwargs)

    def zrevrange(self, start, end, **kwargs):
        """
        Returns the range of items included between ``start`` and ``stop``
        in reverse order (from high to low)

        > s = SortedSet("foo")
        > s.add('a', 10)
        1
        > s.add('b', 20)
        1
        > s.add('c', 30)
        1
        > s.zrevrange(1, 2)
        ['b', 'a']
        > s.delete()
        :param kwargs:
        :param kwargs:
        :param end:
        :param start:
        :param start:
        """
        with self.pipe as pipe:
            return pipe.zrevrange(self._key, start, end, **kwargs)

    # noinspection PyShadowingBuiltins
    def zrangebyscore(self, min, max, **kwargs):
        """
        Returns the range of elements included between the scores (min and max)

        > s = SortedSet("foo")
        > s.add('a', 10)
        1
        > s.add('b', 20)
        1
        > s.add('c', 30)
        1
        > s.zrangebyscore(20, 30)
        ['b', 'c']
        > s.delete()
        :param min: int
        :param max: int
        :param kwargs: dict
        """
        with self.pipe as pipe:
            return pipe.zrangebyscore(self._key, min, max, **kwargs)

    # noinspection PyShadowingBuiltins
    def zrevrangebyscore(self, max, min, **kwargs):
        """
        Returns the range of elements included between the scores (min and max)

        > s = SortedSet("foo")
        > s.add('a', 10)
        1
        > s.add('b', 20)
        1
        > s.add('c', 30)
        1
        > s.zrangebyscore(20, 20)
        ['b']
        > s.delete()
        :param kwargs:
        :param min:
        :param max:
        """
        with self.pipe as pipe:
            return pipe.zrevrangebyscore(self._key, max, min, **kwargs)

    def zcard(self):
        """
        Returns the cardinality of the SortedSet.

        > s = SortedSet("foo")
        > s.add("a", 1)
        1
        > s.add("b", 2)
        1
        > s.add("c", 3)
        1
        > s.zcard()
        3
        > s.delete()
        """
        with self.pipe as pipe:
            return pipe.zcard(self._key)

    def zscore(self, elem):
        """
        Return the score of an element

        > s = SortedSet("foo")
        > s.add("a", 10)
        1
        > s.score("a")
        10.0
        > s.delete()
        :param elem:
        """
        with self.pipe as pipe:
            return pipe.zscore(self._key, elem)

    def zremrangebyrank(self, start, stop):
        """
        Remove a range of element between the rank ``start`` and
        ``stop`` both included.

        :param stop:
        :param start:
        :return: the number of item deleted

        > s = SortedSet("foo")
        > s.add("a", 10)
        1
        > s.add("b", 20)
        1
        > s.add("c", 30)
        1
        > s.zremrangebyrank(1, 2)
        2
        > s.members
        ['a']
        > s.delete()
        """
        with self.pipe as pipe:
            return pipe.zremrangebyrank(self._key, start, stop)

    def zremrangebyscore(self, min_value, max_value):
        """
        Remove a range of element by between score ``min_value`` and
        ``max_value`` both included.

        :param max_value:
        :param min_value:
        :returns: the number of items deleted.

        > s = SortedSet("foo")
        > s.add("a", 10)
        1
        > s.add("b", 20)
        1
        > s.add("c", 30)
        1
        > s.zremrangebyscore(10, 20)
        2
        > s.members
        ['c']
        > s.delete()
        """
        with self.pipe as pipe:
            return pipe.zremrangebyscore(self._key, min_value, max_value)

    def zrank(self, elem):
        """
        Returns the rank of the element.

        > s = SortedSet("foo")
        > s.add("a", 10)
        1
        > s.zrank("a")
        0
        > s.delete()
        :param elem:
        """
        with self.pipe as pipe:
            return pipe.zrank(self._key, elem)

    revrank = zrevrank
    score = zscore
    rank = zrank
    incr_by = zincrby
    add = zadd
    remove = zrem


class Hash(Collection):
    def hlen(self):
        """
        Returns the number of elements in the Hash.
        """
        with self.pipe as pipe:
            return pipe.hlen(self._key)

    def hset(self, member, value):
        """
        Set ``member`` in the Hash at ``value``.

        :param value:
        :param member:
        :returns: 1 if member is a new field and the value has been
                  stored, 0 if the field existed and the value has been
                  updated.

        > h = Hash("foo")
        > h.hset("bar", "value")
        1L
        > h.delete()
        """
        with self.pipe as pipe:
            return pipe.hset(self._key, member, value)

    def hsetnx(self, member, value):
        """
        Set ``member`` in the Hash at ``value``.

        :param value:
        :param member:
        :returns: 1 if member is a new field and the value has been
                  stored, 0 if the field existed and the value has been
                  updated.

        > h = Hash("foo")
        > h.hset("bar", "value")
        1L
        > h.delete()
        """
        with self.pipe as pipe:
            return pipe.hsetnx(self._key, member, value)

    def hdel(self, *members):
        """
        Delete one or more hash field.

        :param members: on or more fields to remove.
        :return: the number of fields that were removed

        > h = Hash("foo")
        > h.hset("bar", "value")
        1L
        > h.hdel("bar")
        1
        > h.delete()
        """
        with self.pipe as pipe:
            return pipe.hdel(self._key, *_parse_values(members))

    def hkeys(self):
        """
        Returns all fields name in the Hash
        """
        with self.pipe as pipe:
            return pipe.hkeys(self._key)

    def hgetall(self):
        """
        Returns all the fields and values in the Hash.

        :rtype: dict
        """
        with self.pipe as pipe:
            return pipe.hgetall(self._key)

    def hvals(self):
        """
        Returns all the values in the Hash

        :rtype: list
        """
        with self.pipe as pipe:
            return pipe.hvals(self._key)

    def hget(self, field):
        """
        Returns the value stored in the field, None if the field doesn't exist.
        :param field:
        """
        with self.pipe as pipe:
            return pipe.hget(self._key, field)

    def hexists(self, field):
        """
        Returns ``True`` if the field exists, ``False`` otherwise.
        :param field:
        """
        with self.pipe as pipe:
            return pipe.hexists(self._key, field)

    def hincrby(self, field, increment=1):
        """
        Increment the value of the field.
        :param increment:
        :param field:
        :returns: the value of the field after incrementation

        > h = Hash("foo")
        > h.hincrby("bar", 10)
        10L
        > h.hincrby("bar", 2)
        12L
        > h.delete()
        """
        with self.pipe as pipe:
            return pipe.hincrby(self._key, field, increment)

    def hmget(self, fields):
        """
        Returns the values stored in the fields.
        :param fields:
        """
        with self.pipe as pipe:
            return pipe.hmget(self.key, fields)

    def hmset(self, mapping):
        """
        Sets or updates the fields with their corresponding values.

        :param mapping: a dict with keys and values
        """
        with self.pipe as pipe:
            return pipe.hmset(self.key, mapping)


class ShardedHash(Collection):
    _shards = 1000

    def __repr__(self):
        return "<%s '%s'>" % (self.__class__.__name__, self.key)

    def _sharded_key(self, member):
        return "%s:%s" % (
            self._key,
            long(hashlib.md5(member).hexdigest(), 16) % self._shards)

    def hlen(self):
        r = DeferredResult()
        with self.pipe as pipe:
            results = [pipe.hlen("%s:%s" % (self.key, i))
                       for i in range(0, self._shards)]

            def cb():
                r.set(sum([ref.result for ref in results]))

            pipe.on_execute(cb)
            return r

    def hset(self, member, value):
        """
        Set ``member`` in the Hash at ``value``.

        :param value:
        :param member:
        :returns: 1 if member is a new field and the value has been
                  stored, 0 if the field existed and the value has been
                  updated.

        > h = Hash("foo")
        > h.hset("bar", "value")
        1L
        > h.delete()
        """
        with self.pipe as pipe:
            return pipe.hset(self._sharded_key(member), member, value)

    def hdel(self, *members):
        """
        Delete one or more hash field.

        :param members: on or more fields to remove.
        :return: the number of fields that were removed

        > h = Hash("foo")
        > h.hset("bar", "value")
        1L
        > h.hdel("bar")
        1
        > h.delete()
        """
        r = DeferredResult()
        with self.pipe as pipe:
            results = [pipe.hdel(self._sharded_key(member), member)
                       for member in _parse_values(members)]

            def cb():
                r.set(sum([ref.result for ref in results]))

            pipe.on_execute(cb)
            return r

    def hget(self, field):
        """
        Returns the value stored in the field, None if the field doesn't exist.
        :param field:
        """
        with self.pipe as pipe:
            return pipe.hget(self._sharded_key(field), field)

    def hexists(self, field):
        """
        Returns ``True`` if the field exists, ``False`` otherwise.
        :param field:
        """
        with self.pipe as pipe:
            return pipe.hexists(self._sharded_key(field), field)

    def hincrby(self, field, increment=1):
        """
        Increment the value of the field.
        :param increment:
        :param field:
        :returns: the value of the field after incrementation

        > h = Hash("foo")
        > h.hincrby("bar", 10)
        10L
        > h.hincrby("bar", 2)
        12L
        > h.delete()
        """
        with self.pipe as pipe:
            return pipe.hincrby(self._sharded_key(field), field, increment)
