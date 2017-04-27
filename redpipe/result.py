from .exceptions import ResultNotReady
import json

__all__ = [
    'Future',
]


class Future(object):
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
    def json(self):
        return json.dumps(self.result)

    @property
    def result(self):
        try:
            return self._result
        except AttributeError:
            pass

        raise ResultNotReady('Wait until after the pipeline executes.')

    def IS(self, other):
        return self.result is other

    def isinstance(self, other):
        return isinstance(self.result, other)

    def id(self):
        return id(self.result)

    def __repr__(self):
        try:
            return repr(self.result)
        except ResultNotReady:
            return repr(None)

    def __str__(self):
        return str(self.result)

    def __lt__(self, other):
        return self.result < other

    def __le__(self, other):
        return self.result <= other

    def __gt__(self, other):
        return self.result > other

    def __ge__(self, other):
        return self.result >= other

    def __hash__(self):
        return hash(self.result)

    def __eq__(self, other):
        return self.result == other

    def __ne__(self, other):
        return self.result != other

    def __nonzero__(self):
        return bool(self.result)

    def __bytes__(self):
        return bytes(self.result)

    def __bool__(self):
        return bool(self.result)

    def __call__(self, *args, **kwargs):
        return self.result(*args, **kwargs)

    def __len__(self):
        return len(self.result)

    def __iter__(self):
        for item in self.result:
            yield item

    def __contains__(self, item):
        return item in self.result

    def __reversed__(self):
        return reversed(self.result)

    def __getitem__(self, item):
        return self.result[item]

    def __int__(self):
        return int(self.result)

    def __float__(self):
        return float(self.result)

    def __round__(self, ndigits=0):
        return round(self.result, ndigits=ndigits)

    def __add__(self, other):
        return self.result + other

    def __sub__(self, other):
        return self.result - other

    def __mul__(self, other):
        return self.result * other

    def __mod__(self, other):
        return self.result % other

    def __div__(self, other):
        return self.result / other

    def __truediv__(self, other):
        return self.result / other

    def __floordiv__(self, other):
        return self.result // other

    def __pow__(self, power, modulo=None):
        return pow(self.result, power, modulo)

    def __lshift__(self, other):
        return self.result << other

    def __rshift__(self, other):
        return self.result >> other

    def __and__(self, other):
        return self.result & other

    def __xor__(self, other):
        return self.result ^ other

    def __or__(self, other):
        return self.result | other

    def __radd__(self, other):
        return other + self.result

    def __rsub__(self, other):
        return other - self.result

    def __rmul__(self, other):
        return self.result * other

    def __rmod__(self, other):
        return other % self.result

    def __rdiv__(self, other):
        return other / self.result

    def __rtruediv__(self, other):
        return other / self.result

    def __rfloordiv__(self, other):
        return other // self.result

    def __rpow__(self, other):
        return other ** self.result

    def __rlshift__(self, other):
        return other << self.result

    def __rrshift__(self, other):
        return other >> self.result

    def __rand__(self, other):
        return other & self.result

    def __rxor__(self, other):
        return other ^ self.result

    def __ror__(self, other):
        return other | self.result

    def __getattr__(self, name, default=None):
        if name[0] == '_':
            raise AttributeError(name)

        return getattr(self.result, name, default)
