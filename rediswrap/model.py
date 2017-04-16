import json
from .context import PipelineContext


class Model(object):
    __slots__ = ['key', '_data']

    _fields = {}

    def __init__(self, key, pipe=None, **kwargs):
        self.key = key
        self._data = {}
        if kwargs:
            self.change(pipe=pipe, **kwargs)
        else:
            with PipelineContext(pipe) as pipe:
                ref = pipe.hgetall(self._key)

                def cb():
                    if not ref.result:
                        return

                    for k, v in ref.result.items():
                        try:
                            v = self._fields[k].from_persistence(v)
                        except (KeyError, AttributeError):
                            pass
                        self._data[k] = v

                pipe.on_execute(cb)

    def change(self, pipe=None, **changes):
        key = self._key
        with PipelineContext(pipe) as pipe:
            for k, v in changes.items():
                try:
                    pv = self._fields[k].to_persistence(v)
                except KeyError:
                    pv = v

                if pv is None:
                    pipe.hdel(key, k)
                    try:
                        del self._data[k]
                    except KeyError:
                        pass
                else:
                    pipe.hset(key, k, pv)
                    self._data[k] = v

    @property
    def exists(self):
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

    def __getattr__(self, item):
        if item[0] == '_':
            raise AttributeError('attribute not found: %s' % item)
        return self._data[item]

    def __str__(self):
        if self._data:
            return "<%s-%s>" % (self.__class__.__name__, self.key)
        else:
            return ''

    def __repr__(self):
        if self._data:
            data = {k: v for k, v in self._data.items()}
            data['_key'] = self.key
            return json.dumps(data)
        else:
            return ''
