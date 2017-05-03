# -*- coding: utf-8 -*-
"""
Experimental code based on patterns I've used elsewhere.
Makes it possible to load data from redis as an object and access the fields.
Then store changes back into redis.
"""
import json
from six import add_metaclass
from .pipelines import pipeline
from .keyspaces import Hash
from .fields import TextField
from .exceptions import InvalidOperation
from .futures import Future

__all__ = ['Struct']


class StructMeta(type):
    """
    Data binding of a redpipe.Hash to the core of the Struct object.
    Creates it dynamically on class construction.
    uses the _keyspace and _connection fields
    Meta Classes are strange beasts.
    """

    def __new__(mcs, name, bases, d):
        if name in ['Struct']:
            return type.__new__(mcs, name, bases, d)

        class StructHash(Hash):
            _keyspace = d.get('_keyspace', name)
            _connection = d.get('_connection', None)
            _fields = d.get('_fields', {})
            _keyparse = d.get('_keyparse', TextField)
            _valueparse = d.get('_valueparse', TextField)
            _memberparse = d.get('_memberparse', TextField)

        d['core'] = StructHash

        return type.__new__(mcs, name, bases, d)


@add_metaclass(StructMeta)
class Struct(object):
    """
    load and store structured data in redis using OOP patterns.
    """
    __slots__ = ['key', '_data']
    _keyspace = None
    _connection = None
    _key_name = None
    _fields = {}

    def __init__(self, _key_or_data=None, pipe=None, **kwargs):

        keyname = self.key_name

        if pipe is None and not kwargs:
            try:
                coerced = dict(_key_or_data)
                self.key = coerced[keyname]
                del coerced[keyname]
                self._data = coerced
                return
            except KeyError:
                raise InvalidOperation(
                    'must specify primary key when copying a struct')
            except (ValueError, TypeError):
                pass

        self.key = _key_or_data
        self._data = {}
        with self._pipe(pipe) as pipe:
            if kwargs:
                coerced = dict(kwargs)
                if self.key is None:
                    self.key = coerced[self.key_name]
                    del coerced[self.key_name]

                self.update(coerced, pipe=pipe)

            ref = self.core(pipe=pipe).hgetall(self.key)

            def cb():
                if not ref.result:
                    return

                for k, v in ref.result.items():
                    self._data[k] = v

            pipe.on_execute(cb)

    @property
    def key_name(self):
        return self._key_name or '_key'

    def incr(self, field, amount=1, pipe=None):
        with self._pipe(pipe) as pipe:
            core = self.core(pipe=pipe)
            core.hincrby(self.key, field, amount)
            ref = core.hget(self.key, field)

            def cb():
                self._data[field] = ref.result

            pipe.on_execute(cb)

    def decr(self, field, amount=1, pipe=None):
        self.incr(field, amount * -1, pipe=pipe)

    def update(self, changes, pipe=None):
        if self.key_name in changes:
            raise InvalidOperation('cannot update the redis key')

        with self._pipe(pipe) as pipe:
            core = self.core(pipe=pipe)

            def build(k, v):
                if v is None:
                    core.hdel(self.key, k)
                else:
                    core.hset(self.key, k, v)

                def cb():
                    if v is None:
                        try:
                            del self._data[k]
                        except KeyError:
                            pass
                    else:
                        self._data[k] = v

                pipe.on_execute(cb)

            for k, v in changes.items():
                build(k, v)

    def remove(self, fields, pipe=None):
        if self.key_name in fields:
            raise InvalidOperation('cannot remove the redis key')

        with self._pipe(pipe) as pipe:
            core = self.core(pipe=pipe)
            core.hdel(self.key, *fields)

            def cb():
                for k in fields:
                    try:
                        del self._data[k]
                    except KeyError:
                        pass

            pipe.on_execute(cb)

    def copy(self):
        return self.__class__(dict(self))

    @property
    def persisted(self):
        return True if self._data else False

    def clear(self, pipe=None):
        with self._pipe(pipe) as pipe:
            self.core(pipe=pipe).delete(self.key)

            def cb():
                self._data = {}

            pipe.on_execute(cb)

    def get(self, item, default=None):
        return self._data.get(item, default)

    def pop(self, name, default=None, pipe=None):
        f = Future()
        with self._pipe(pipe) as pipe:
            c = self.core(pipe)
            ref = c.hget(self.key, name)
            c.hdel(self.key, name)

            def cb():
                f.set(default if ref.result is None else ref.result)
                self._data.pop(name)

            pipe.on_execute(cb)

        return f

    def _pipe(self, pipe=None):
        return pipeline(pipe, name=self._connection,
                        autocommit=True)

    def __getitem__(self, item):
        if item == self.key_name:
            return self.key

        return self._data[item]

    def __delitem__(self, key):
        tpl = 'cannot delete %s from %s indirectly. Use the delete method.'
        raise InvalidOperation(tpl % (key, self))

    def __setitem__(self, key, value):
        tpl = 'cannot set %s key on %s indirectly. Use the set method.'
        raise InvalidOperation(tpl % (key, self))

    def __iter__(self):
        for k in self.keys():
            yield k

    def __len__(self):
        return len(dict(self))

    def __contains__(self, item):
        if item == self.key_name:
            return True
        return item in self._data

    def items(self):
        yield self.key_name, self.key
        for k, v in self._data.items():
            yield k, v

    iteritems = items

    def __eq__(self, other):
        if self is other:
            return True
        try:
            if dict(self) == dict(other):
                return True
        except (TypeError, ValueError):
            pass

        return False

    def keys(self):
        return [row[0] for row in self.items()]

    def __str__(self):
        return "<%s:%s>" % (self.__class__.__name__, self.key)

    def __repr__(self):
        return json.dumps(dict(self))
