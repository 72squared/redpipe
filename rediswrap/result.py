import json


class DeferredResult(object):
    __slots__ = ['result']

    def set(self, data):
        self.result = data

    def __repr__(self):
        try:
            return json.dumps(self.result)
        except AttributeError:
            return ''


class InstantResult(object):
    __slots__ = ['result']

    def __init__(self, data=None):
        self.result = data

    def __repr__(self):
        return json.dumps(self.result)
