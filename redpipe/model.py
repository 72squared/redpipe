from six import add_metaclass
from .pipeline import pipeline
from .fields import TextField
from .exceptions import InvalidFieldValue
from .collections import Hash

__all__ = ['Model']


class ModelMeta(type):
    def __new__(mcs, name, bases, d):
        if name in ['Model']:
            return type.__new__(mcs, name, bases, d)

        class ModelHash(Hash):
            _keyspace = d.get('_keyspace', name)
            _connection = d.get('_connection', None)

        d['core'] = ModelHash

        model = type.__new__(mcs, name, bases, d)
        return model


@add_metaclass(ModelMeta)
class Model(object):
    __metaclass__ = ModelMeta
    __slots__ = ['key', '_data']
    _keyspace = None
    _connection = None
    _fields = {}

    def __init__(self, key, pipe=None, **kwargs):
        self.key = key
        self._data = {}
        with pipeline(pipe, name=self._connection, autocommit=True) as pipe:
            if kwargs:
                self.save(pipe=pipe, **kwargs)

            self.load(pipe=pipe)

    def load(self, pipe=None):
        with pipeline(pipe, name=self._connection, autocommit=True) as pipe:
            ref = self.core(self.key, pipe=pipe).hgetall()

            def cb():
                if not ref.result:
                    return

                for k, v in ref.result.items():
                    v = self._from_persistence(k, v)
                    self._data[k] = v

            pipe.on_execute(cb)

    def save(self, pipe=None, **changes):
        with pipeline(pipe, name=self._connection, autocommit=True) as pipe:
            core = self.core(self.key, pipe=pipe)

            def build(k, v):
                pv = self._to_persistence(k, v) if v is not None else None
                if pv is None:
                    core.hdel(k)
                else:
                    core.hset(k, pv)

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

    @property
    def persisted(self):
        return True if self._data else False

    def delete(self, pipe=None):
        with pipeline(pipe, name=self._connection, autocommit=True) as pipe:
            self.core(self.key, pipe=pipe).delete()

            def cb():
                self._data = {}

            pipe.on_execute(cb)

    def _to_persistence(self, k, v):
        try:
            field_validator = self._fields[k]
            if not field_validator.validate(v):
                raise InvalidFieldValue('invalid value for field %s' % k)
            return field_validator.to_persistence(v)
        except KeyError:
            return TextField().to_persistence(v)

    def _from_persistence(self, k, v):
        try:
            field_validator = self._fields[k]
            return field_validator.from_persistence(v)
        except KeyError:
            return v

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
