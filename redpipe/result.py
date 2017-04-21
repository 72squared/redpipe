from .exceptions import ResultNotReady

__all__ = [
    'Deferred',
]


class Deferred(object):
    """
    An object returned from all our Pipeline calls.
    """
    __slots__ = ['_result']

    def set(self, data):
        """
        Write the data into the object.
        Note that I intentionally did not declare `result` in
        the constructor.
        I want an error to happen if you try to access it
        before it is set.

        :param data:
        :return:
        """
        self._result = data

    @property
    def result(self):
        try:
            return self._result
        except AttributeError:
            pass

        raise ResultNotReady('Wait until after the pipeline executes.')
