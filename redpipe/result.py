__all__ = [
    'DeferredResult',
    'InstantResult'
]


class DeferredResult(object):
    __slots__ = ['result']

    def set(self, data):
        self.result = data


class InstantResult(object):
    __slots__ = ['result']

    def __init__(self, data=None):
        self.result = data
