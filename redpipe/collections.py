from .pipeline import pipeline
from .lua import lua_restorenx, lua_object_info
from .exceptions import InvalidOperation

__all__ = """
String
Set
List
SortedSet
Hash
""".split()


class Collection(object):
    """
    Base class for all collections.
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

    @property
    def _key(self):
        """
        Get the key we pass to redis.
        If no namespace is declared, it will use the class name.
        :return: str
        """
        namespace = self._keyspace or self.__class__.__name__
        return "%s{%s}" % (namespace, self.key)

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
        :return: DeferredResult()
        """
        with self.pipe as pipe:
            return pipe.delete(self._key)

    def expire(self, time):
        """
        Allow the key to expire after ``time`` seconds.
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
            return pipe.eval(script, 1, self._key, *args)

    def dump(self):
        """
        get a redis RDB-like serialization of the object.
        :return: DeferredResult()
        """
        with self.pipe as pipe:
            return pipe.dump(self._key)

    def restore(self, data, pttl=0):
        """
        Restore serialized dump of a key back into redis
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
    """
    Manipulate a String key in Redis.
    """
    def get(self):
        """
        set the value as a string in the key
        :return: DeferredResult()
        """
        with self.pipe as pipe:
            return pipe.get(self._key)

    def set(self, value):
        """
        set the value as a string in the key
        :param value:
        :return: DeferredResult()
        """
        with self.pipe as pipe:
            return pipe.set(self._key, value)

    def incr(self):
        """
        increment the value for key by 1
        :return: DeferredResult()
        """
        with self.pipe as pipe:
            return pipe.incr(self._key)

    def incrby(self, value=1):
        """
        increment the value for key by value: int
        :param value: int
        :return: DeferredResult()
        """
        with self.pipe as pipe:
            return pipe.incrby(self._key, value)

    def incrbyfloat(self, value=1.0):
        """
        increment the value for key by value: float
        :param value: int
        :return: DeferredResult()
        """
        with self.pipe as pipe:
            return pipe.incrbyfloat(self._key, value)

    def setnx(self, value):
        """
        Set the value as a string in the key only if the key doesn't exist.
        :param value:
        :return: DeferredResult()
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
    Manipulate a Set key in redis.
    """

    def sadd(self, *values):
        """
        Add the specified members to the Set.
        :param values: a list of values or a simple value.
        :return: DeferredResult()
        """
        with self.pipe as pipe:
            return pipe.sadd(self._key, *_parse_values(values))

    def srem(self, *values):
        """
        Remove the values from the Set if they are present.

        :param values: a list of values or a simple value.
        :return: DeferredResult()
        """
        with self.pipe as pipe:
            return pipe.srem(self._key, *_parse_values(values))

    def spop(self):
        """
        Remove and return (pop) a random element from the Set.

        :return: DeferredResult()
        """
        with self.pipe as pipe:
            return pipe.spop(self._key)

    def smembers(self):
        with self.pipe as pipe:
            return pipe.smembers(self._key)

    members = smembers

    def scard(self):
        """
        How many items in the set?

        :return: DeferredResult()

        """
        with self.pipe as pipe:
            return pipe.scard(self._key)

    def sismember(self, value):
        """
        Is the provided value is in the ``Set`?
        :param value:
        :return: DeferredResult()
        """
        with self.pipe as pipe:
            return pipe.sismember(self._key, value)

    def srandmember(self):
        """
        Return a random member of the set.
        :return: DeferredResult()
        """
        with self.pipe as pipe:
            return pipe.srandmember(self._key)

    add = sadd
    pop = spop
    remove = srem


class List(Collection):
    """
    Manipulate a List key in redis
    """

    def members(self):
        """
        Returns all items in the list.
        :return: DeferredResult()
        """
        return self.lrange(0, -1)

    def llen(self):
        """
        Returns the length of the list.
        :return: DeferredResult()
        """
        with self.pipe as pipe:
            return pipe.llen(self._key)

    def lrange(self, start, stop):
        """
        Returns a range of items.

        :param start: integer representing the start index of the range
        :param stop: integer representing the size of the list.
        :return: DeferredResult()
        """
        with self.pipe as pipe:
            return pipe.lrange(self._key, start, stop)

    def lpush(self, *values):
        """
        Push the value into the list from the *left* side

        :param values: a list of values or single value to push
        :return: DeferredResult()
        """
        with self.pipe as pipe:
            return pipe.lpush(self._key, *_parse_values(values))

    def rpush(self, *values):
        """
        Push the value into the list from the *right* side

        :param values: a list of values or single value to push
        :return: DeferredResult()
        """
        with self.pipe as pipe:
            return pipe.rpush(self._key, *_parse_values(values))

    def lpop(self):
        """
        Pop the first object from the left.

        :return: DeferredResult()

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
        :return: DeferredResult()
        """
        with self.pipe as pipe:
            return pipe.lrem(self._key, num, value)

    def ltrim(self, start, end):
        """
        Trim the list from start to end.

        :param start:
        :param end:
        :return: DeferredResult()
        """
        with self.pipe as pipe:
            return pipe.ltrim(self._key, start, end)

    def lindex(self, idx):
        """
        Return the value at the index *idx*

        :param idx: the index to fetch the value.
        :return: DeferredResult()
        """
        with self.pipe as pipe:
            return pipe.lindex(self._key, idx)

    def lset(self, idx, value=0):
        """
        Set the value in the list at index *idx*

        :param value:
        :param idx:
        :return: DeferredResult()
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
    Manipulate a SortedSet key in redis.
    """

    def members(self):
        """
        Returns the members of the set.
        :return: DeferredResult()
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
        :return: DeferredResult()
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
        """
        with self.pipe as pipe:
            return pipe.zrem(self._key, *_parse_values(values))

    def zincrby(self, att, value=1):
        """
        Increment the score of the item by `value`
        :param att: the member to increment
        :param value: the value to add to the current score
        :returns: the new score of the member


        """
        with self.pipe as pipe:
            return pipe.zincrby(self._key, att, value)

    def zrevrank(self, member):
        """
        Returns the ranking in reverse order for the member
        :param member: str
        """
        with self.pipe as pipe:
            return pipe.zrevrank(self._key, member)

    def zrange(self, start, end, **kwargs):
        """
        Returns all the elements including between ``start`` (non included) and
        ``stop`` (included).
        """
        with self.pipe as pipe:
            return pipe.zrange(self._key, start, end, **kwargs)

    def zrevrange(self, start, end, **kwargs):
        """
        Returns the range of items included between ``start`` and ``stop``
        in reverse order (from high to low)
        :param kwargs:
        :param kwargs:
        :param end:
        :param start:
        :param start:
        """
        with self.pipe as pipe:
            return pipe.zrevrange(self._key, start, end, **kwargs)

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
        :return: DeferredResult()
        """
        with self.pipe as pipe:
            return pipe.zrangebyscore(self._key,
                                      min,
                                      max,
                                      start=start,
                                      num=num,
                                      withscores=withscores,
                                      score_cast_func=score_cast_func)

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
        :return: DeferredResult()
        """
        with self.pipe as pipe:
            return pipe.zrevrangebyscore(self._key,
                                         max, min,
                                         start=start,
                                         num=num,
                                         withscores=withscores,
                                         score_cast_func=score_cast_func)

    def zcard(self):
        """
        Returns the cardinality of the SortedSet.
        :return: DeferredResult()
        """
        with self.pipe as pipe:
            return pipe.zcard(self._key)

    def zscore(self, elem):
        """
        Return the score of an element
        :param elem:
        :return: DeferredResult()
        """
        with self.pipe as pipe:
            return pipe.zscore(self._key, elem)

    def zremrangebyrank(self, start, stop):
        """
        Remove a range of element between the rank ``start`` and
        ``stop`` both included.
        :param stop:
        :param start:
        :return: DeferredResult()
        """
        with self.pipe as pipe:
            return pipe.zremrangebyrank(self._key, start, stop)

    def zremrangebyscore(self, min_value, max_value):
        """
        Remove a range of element by between score ``min_value`` and
        ``max_value`` both included.
        :param max_value:
        :param min_value:
        :return: DeferredResult()
        """
        with self.pipe as pipe:
            return pipe.zremrangebyscore(self._key, min_value, max_value)

    def zrank(self, elem):
        """
        Returns the rank of the element.
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
    """
    Manipulate a Hash key in Redis.
    """
    def hlen(self):
        """
        Returns the number of elements in the Hash.
        :return: DeferredResult()
        """
        with self.pipe as pipe:
            return pipe.hlen(self._key)

    def hset(self, member, value):
        """
        Set ``member`` in the Hash at ``value``.

        :param value:
        :param member:
        :return: DeferredResult()
        """
        with self.pipe as pipe:
            return pipe.hset(self._key, member, value)

    def hsetnx(self, member, value):
        """
        Set ``member`` in the Hash at ``value``.

        :param value:
        :param member:
        :return: DeferredResult()
        """
        with self.pipe as pipe:
            return pipe.hsetnx(self._key, member, value)

    def hdel(self, *members):
        """
        Delete one or more hash field.

        :param members: on or more fields to remove.
        :return: DeferredResult()
        """
        with self.pipe as pipe:
            return pipe.hdel(self._key, *_parse_values(members))

    def hkeys(self):
        """
        Returns all fields name in the Hash
        return: DeferredResult()
        """
        with self.pipe as pipe:
            return pipe.hkeys(self._key)

    def hgetall(self):
        """
        Returns all the fields and values in the Hash.
        :return: DeferredResult()
        """
        with self.pipe as pipe:
            return pipe.hgetall(self._key)

    def hvals(self):
        """
        Returns all the values in the Hash
        :return: DeferredResult()
        """
        with self.pipe as pipe:
            return pipe.hvals(self._key)

    def hget(self, field):
        """
        Returns the value stored in the field, None if the field doesn't exist.
        :param field:
        :return: DeferredResult()
        """
        with self.pipe as pipe:
            return pipe.hget(self._key, field)

    def hexists(self, field):
        """
        Returns ``True`` if the field exists, ``False`` otherwise.
        :param field:
        :return: DeferredResult()
        """
        with self.pipe as pipe:
            return pipe.hexists(self._key, field)

    def hincrby(self, field, increment=1):
        """
        Increment the value of the field.
        :param increment:
        :param field:
        :return: DeferredResult()
        """
        with self.pipe as pipe:
            return pipe.hincrby(self._key, field, increment)

    def hmget(self, fields):
        """
        Returns the values stored in the fields.
        :param fields:
        :return: DeferredResult()
        """
        with self.pipe as pipe:
            return pipe.hmget(self._key, fields)

    def hmset(self, mapping):
        """
        Sets or updates the fields with their corresponding values.
        :param mapping: a dict with keys and values
        :return: DeferredResult()
        """
        with self.pipe as pipe:
            return pipe.hmset(self._key, mapping)
