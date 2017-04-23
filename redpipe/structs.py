from six import add_metaclass
from .pipeline import pipeline
from .datatypes import Hash

__all__ = ['Struct']


class StructMeta(type):
    def __new__(mcs, name, bases, d):
        if name in ['Struct']:
            return type.__new__(mcs, name, bases, d)

        class StructHash(Hash):
            _keyspace = d.get('_keyspace', name)
            _connection = d.get('_connection', None)
            _fields = d.get('_fields', {})

        d['core'] = StructHash

        return type.__new__(mcs, name, bases, d)


@add_metaclass(StructMeta)
class Struct(object):
    __slots__ = ['key', '_data']
    _keyspace = None
    _connection = None
    _fields = {}

    def __init__(self, key, pipe=None, **kwargs):
        self.key = key
        self._data = {}
        with pipeline(pipe, name=self._connection, autocommit=True) as pipe:
            if kwargs:
                self.change(pipe=pipe, **kwargs)

            self.load(pipe=pipe)

    def load(self, pipe=None):
        with pipeline(pipe, name=self._connection, autocommit=True) as pipe:
            ref = self.core(self.key, pipe=pipe).hgetall()

            def cb():
                if not ref.result:
                    return

                for k, v in ref.result.items():
                    self._data[k] = v

            pipe.on_execute(cb)

    def change(self, pipe=None, **changes):
        with pipeline(pipe, name=self._connection, autocommit=True) as pipe:
            core = self.core(self.key, pipe=pipe)

            def build(k, v):
                pv = None if v is None else core.to_redis(k, v)
                if pv is None:
                    core.hdel(k)
                else:
                    core.hset(k, v)

                def cb():
                    if pv is None:
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
        with pipeline(pipe, name=self._connection, autocommit=True) as pipe:
            core = self.core(self.key, pipe=pipe)
            core.hdel(*fields)

            def cb():
                for k in fields:
                    try:
                        del self._data[k]
                    except KeyError:
                        pass

            pipe.on_execute(cb)

    @property
    def persisted(self):
        return True if self._data else False

    def delete(self, pipe=None):
        with pipeline(pipe, name=self._connection, autocommit=True) as pipe:
            self.core(self.key, pipe=pipe).delete()

            def cb():
                self._data = {}

            pipe.on_execute(cb)

    def get(self, item, default=None):
        return self._data.get(item, default)

    def __getattr__(self, item):
        if item == '_key':
            return self.key

        try:
            return self._data[item]
        except KeyError:
            pass

        if item in self._fields:
            return None

        raise AttributeError("%s not found in %s" %
                             (item, self.__class__.__name__))

    def __getitem__(self, item):
        return self.key if item == '_key' else self._data[item]

    def __iter__(self):
        for k, v in self.items():
            yield k, v

    def items(self):
        yield '_key', self.key
        for k, v in self._data.items():
            yield k, v

    def __str__(self):
        return "<%s:%s>" % (self.__class__.__name__, self.key)
