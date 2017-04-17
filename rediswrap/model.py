from .context import PipelineContext
from .fields import TextField
from .exceptions import InvalidFieldValue


class Model(object):
    __slots__ = ['key', '_data']

    _fields = {}

    def __init__(self, key, pipe=None, **kwargs):
        self.key = key
        self._data = {}
        with PipelineContext(pipe) as pipe:
            if kwargs:
                self.save(pipe=pipe, **kwargs)

            self.load(pipe=pipe)

    def load(self, pipe=None):
        with PipelineContext(pipe) as pipe:
            ref = pipe.hgetall(self._key)

            def cb():
                if not ref.result:
                    return

                for k, v in ref.result.items():
                    v = v.decode('utf8')
                    k = k.decode('utf8')
                    try:
                        v = self._from_persistence(k, v)
                    except (KeyError, AttributeError):
                        pass
                    self._data[k] = v

            pipe.on_execute(cb)

    def save(self, pipe=None, **changes):
        key = self._key
        with PipelineContext(pipe) as pipe:
            def build(k, v):
                pv = self._to_persistence(k, v) if v is not None else None
                if pv is None:
                    pipe.hdel(key, k)
                else:
                    pipe.hset(key, k, pv)

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

    @property
    def _key(self):
        try:
            namespace = self._namespace
        except AttributeError:
            namespace = self.__class__.__name__

        return "%s{%s}" % (namespace, self.key)

    def delete(self, pipe=None):
        with PipelineContext(pipe) as pipe:
            pipe.delete(self._key)

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
            return TextField().from_persistence(v)

    def get(self, item, default=None):
        return self._data.get(item, default)

    def __getattr__(self, item):
        try:
            return self.key if item == '_key' else self._data[item]
        except KeyError:
            raise AttributeError(item)

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
        return "<%s-%s>" % (self.__class__.__name__, self.key)
