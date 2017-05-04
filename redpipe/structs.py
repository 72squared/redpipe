# -*- coding: utf-8 -*-
"""
Experimental code based on patterns I've used elsewhere.
Makes it possible to load data from redis as an object and access the fields.
Then store changes back into redis.
"""
from six import add_metaclass
from json.encoder import JSONEncoder
from functools import wraps
from .pipelines import autoexec
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

    @classmethod
    def delete(cls, keys, pipe=None):
        with cls._pipe(pipe) as pipe:
            core = cls.core(pipe)
            core.delete(*keys)

    @classmethod
    def _pipe(cls, pipe=None):
        return autoexec(pipe, name=cls._connection)

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

    def iteritems(self):
        yield self.key_name, self.key
        for k, v in self._data.items():
            yield k, v

    def items(self):
        return [row for row in self.iteritems()]

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
        return repr(dict(self))

    def __getstate__(self):
        return self.key, self._data,

    def __setstate__(self, state):
        self.key = state[0]
        self._data = state[1]

    @property
    def _redpipe_struct_as_dict(self):
        return dict(self)


def _json_default_encoder(func):
    """
    Monkey-Patch the core json encoder library.
    This isn't as bad as it sounds.
    We override the default method so that if an object
    falls through and can't be encoded normally, we see if it is
    a Future object and return the result to be encoded.

    I set a special attribute on the Struct object so I can tell
    that's what it is.

    If that doesn't work, I fall back to the earlier behavior.
    The nice thing about patching the library this way is that it
    won't inerfere with existing code and it can itself be wrapped
    by other methods.

    So it's very extensible.

    :param func: the JSONEncoder.default method.
    :return: an object that can be json serialized.
    """

    @wraps(func)
    def inner(self, o):
        try:
            return o._redpipe_struct_as_dict  # noqa
        except AttributeError:
            pass
        return func(self, o)

    return inner


JSONEncoder.default = _json_default_encoder(JSONEncoder.default)
