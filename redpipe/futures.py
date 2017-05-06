# -*- coding: utf-8 -*-
"""
The `Future()` object in **RedPipe** gives us the ability to make the pipeline
interface of redis-py look like the non-pipelined interface.
You call a command and get a response back.
Only the response is not the actual data.
It is an empty container called a `Future`.
There is a callback attached to that empty container.
When the pipeine is executed, the pipeline injects the response into the
container.

This Future container is a very special kind of python object.
It can imitate anything it contains.
If there is an integer inside, it behaves like an integer.
If it holds a dictionary, it behaves like a dictionary.
If it holds a list, it behaves like a list.
Your application should be able to use it interchangeably.

There are a few gotchas to watch out for:

* isinstance() checks
* identity checks like: future is None
* trying to mutate the object like this: future += 1

You can always type cast the object into the type you expect
if you need this behavior.

.. code-block:: python

    f = Future()
    f.set(1)

    # f is 1 fails
    assert(int(f) is 1)

This doesn't work so well for is None checks.
You can use equality checks though.
Or you can use our handy IS method.
Or you can access the underlying result

.. code-block:: python

    f = Future()
    f.set(None)

    assert(f == None)
    assert(f.IS(None))
    assert(f.result is None)

Hope that helps.

Other than those few caveats, you should be able to access a future object
just like the underlying result.

Here are some examples if your result is numeric.

.. code-block:: python

    future = Future()
    future.set(1)
    assert(future == 1)
    assert(future != 2)
    assert(bool(future))
    assert(float(future) == 1.0)
    assert(future + 1 == 2)
    assert(future * 2 == 2)
    assert(future ^ 1 == 0)
    assert(repr(future) == '1')

And here is an example if your future is a list:

.. code-block:: python

    future = Future()
    future.set([1])
    assert(future == [1])
    assert([v for v in future] == [1])
    assert(future + [2] == [1, 2])

And here is a dictionary:

.. code-block:: python

    future = Future()
    future.set({'a': 1})
    assert(future == {'a': 1})
    assert(dict(future) == {'a': 1})
    assert({k: v for k, v in future.items()} == {'a': 1})

There are many more operations supported but these are the most common.
`Let me know <https://github.com/72squared/redpipe/issues>`_ if you need
more examples or explanation.
"""

from .exceptions import ResultNotReady
from json.encoder import JSONEncoder
from functools import wraps

__all__ = [
    'Future',
    'IS',
    'ISINSTANCE'
]


def IS(instance, other):  # noqa
    """
    Support the `future is other` use-case.
    Can't override the language so we built a function.
    Will work on non-future objects too.

    :param instance: future or any python object
    :param other: object to compare.
    :return:
    """
    try:
        instance = instance._redpipe_future_result  # noqa
    except AttributeError:
        pass

    try:
        other = other._redpipe_future_result
    except AttributeError:
        pass

    return instance is other


def ISINSTANCE(instance, A_tuple):  # noqa
    """
    Allows you to do isinstance checks on futures.
    Really, I discourage this because duck-typing is usually better.
    But this can provide you with a way to use isinstance with futures.
    Works with other objects too.

    :param instance:
    :param A_tuple:
    :return:
    """
    try:
        instance = instance._redpipe_future_result
    except AttributeError:
        pass

    return isinstance(instance, A_tuple)


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

        :param data: any python object
        :return: None
        """
        self._result = data

    @property
    def result(self):
        """
        Get the underlying result.
        Usually one of the data types returned by redis-py.

        :return: None, str, int, list, set, dict
        """
        try:
            return self._result
        except AttributeError:
            pass

        raise ResultNotReady('Wait until after the pipeline executes.')

    def IS(self, other):
        """
        Allows you to do identity comparisons on the underlying object.

        :param other: Mixed
        :return: bool
        """
        return self.result is other

    def isinstance(self, other):
        """
        allows you to check the instance type of the underlying result.

        :param other:
        :return:
        """
        return isinstance(self.result, other)

    def id(self):
        """
        Get the object id of the underlying result.
        """
        return id(self.result)

    def __repr__(self):
        """
        Magic method in python used to override the behavor of repr(future)

        :return: str
        """
        try:
            return repr(self.result)
        except ResultNotReady:
            return repr(None)

    def __str__(self):
        """
        Magic method in python used to override the behavor of str(future)

        :return:
        """
        return str(self.result)

    def __lt__(self, other):
        """
        Magic method in python used to override the behavor of future < other

        :param other: Any python object, usually numeric
        :return: bool
        """
        return self.result < other

    def __le__(self, other):
        """
        Magic method in python used to override the behavor of future <= other


        :param other: Any python object, usually numeric
        :return: bool
        """
        return self.result <= other

    def __gt__(self, other):
        """
        Magic method in python used to override the behavor of future > other

        :param other: Any python object, usually numeric
        :return: bool
        """
        return self.result > other

    def __ge__(self, other):
        """
        Magic method in python used to override the behavor of future >= other

        :param other: Any python object, usually numeric
        :return: bool
        """
        return self.result >= other

    def __hash__(self):
        """
        Magic method in python used to override the behavor of hash(future)

        :return: int
        """
        return hash(self.result)

    def __eq__(self, other):
        """
        Magic method in python used to override the behavor of future == other

        :param other: Any python object
        :return: bool
        """
        return self.result == other

    def __ne__(self, other):
        """
        Magic method in python used to override the behavor of future != other

        :param other: Any python object
        :return: bool
        """
        return self.result != other

    def __nonzero__(self):
        """
        Magic method in python used to override the behavor of bool(future)

        :return: bool
        """
        return bool(self.result)

    def __bytes__(self):
        """
        Magic method in python used to coerce object: bytes(future)

        :return: bytes
        """
        return bytes(self.result)

    def __bool__(self):
        """
        Magic method in python used to coerce object: bool(future)

        :return: bool
        """
        return bool(self.result)

    def __call__(self, *args, **kwargs):
        """
        Magic method in python used to invoke a future:
        future(*args, **kwargs)

        :param args: tuple
        :param kwargs: dict
        :return: Unknown, defined by object
        """
        return self.result(*args, **kwargs)

    def __len__(self):
        """
        Magic method in python used to determine length: len(future)

        :return: int
        """
        return len(self.result)

    def __iter__(self):
        """
        Magic method in python to support iteration.
        Example:

        .. code-block:: python

            future = Future()
            future.set([1, 2, 3])
            for row in future:
                print(row)

        :return: iterable generator
        """
        for item in self.result:
            yield item

    def __contains__(self, item):
        """
        Magic python method supporting: `item in future`

        :param item: any python object
        :return: bool
        """
        return item in self.result

    def __reversed__(self):
        """
        Magic python method to emulate: reversed(future)

        :return: list
        """
        return reversed(self.result)

    def __getitem__(self, item):
        """
        Used to emulate dictionary access of an element: future[key]

        :param item: usually str, key name of dict.

        :return: element, type unknown
        """
        return self.result[item]

    def __int__(self):
        """
        Magic method in python to coerce to int:  int(future)

        :return:
        """
        return int(self.result)

    def __float__(self):
        """
        Magic method in python to coerce to float: float(future)

        :return: float
        """
        return float(self.result)

    def __round__(self, ndigits=0):
        """
        Magic method in python to round: round(future, 1)

        :param ndigits: int
        :return: float, int
        """
        return round(self.result, ndigits=ndigits)

    def __add__(self, other):
        """
        support addition:  result = future + 1

        :param other: int, float, str, list

        :return: int, float, str, list
        """
        return self.result + other

    def __sub__(self, other):
        """
        support subtraction: result = future - 1

        :param other: int, float, str, list
        :return: int, float, str, list
        """
        return self.result - other

    def __mul__(self, other):
        """
        support multiplication: result = future * 2

        :param other: int, float, str, list
        :return: int, float, str, list
        """
        return self.result * other

    def __mod__(self, other):
        """
        support modulo: result = future % 2

        :param other: int, float, str, list
        :return: int, float, str, list
        """
        return self.result % other

    def __div__(self, other):
        """
        support division: result = future / 2
        for python 2

        :param other: int, float
        :return: int, float
        """
        return self.result / other

    def __truediv__(self, other):
        """
        support division: result = future / 2
        for python 3

        :param other: int, float
        :return: int, float
        """
        return self.result / other

    def __floordiv__(self, other):
        """
        support floor division: result = future // 2

        :param other: int, float
        :return: int, float
        """
        return self.result // other

    def __pow__(self, power, modulo=None):
        """
        supports raising to a power: result = pow(future, 3)

        :param power: int
        :param modulo:
        :return: int, float
        """
        return pow(self.result, power, modulo)

    def __lshift__(self, other):
        """
        bitwise operation: result = future << other
        """
        return self.result << other

    def __rshift__(self, other):
        """
        bitwise operation: result = future >> other
        """
        return self.result >> other

    def __and__(self, other):
        """
        bitwise operation: result = future & other
        """
        return self.result & other

    def __xor__(self, other):
        """
        bitwise operation: result = future ^ other
        """
        return self.result ^ other

    def __or__(self, other):
        """
        bitwise operation: result = future | other
        """
        return self.result | other

    def __radd__(self, other):
        """
        addition operation: result = other + future
        """
        return other + self.result

    def __rsub__(self, other):
        """
        subtraction operation: result = other - future
        """
        return other - self.result

    def __rmul__(self, other):
        """
        multiplication operation: result = other * future
        """
        return self.result * other

    def __rmod__(self, other):
        """
        use as modulo: result = other * future
        """
        return other % self.result

    def __rdiv__(self, other):
        """
        use as divisor: result = other / future

        python 2
        """
        return other / self.result

    def __rtruediv__(self, other):
        """
        use as divisor: result = other / future

        python 3
        """
        return other / self.result

    def __rfloordiv__(self, other):
        """
        floor divisor: result other // future
        """
        return other // self.result

    def __rpow__(self, other):
        """
        reverse power: other ** future
        """
        return other ** self.result

    def __rlshift__(self, other):
        """
        result = other << future
        """
        return other << self.result

    def __rrshift__(self, other):
        """
        result = other >> future
        """
        return other >> self.result

    def __rand__(self, other):
        """
        result = other & future
        """
        return other & self.result

    def __rxor__(self, other):
        """
        result = other ^ future
        """
        return other ^ self.result

    def __ror__(self, other):
        """
        result = other | future
        """
        return other | self.result

    def __getattr__(self, name, default=None):
        """
        access an attribute of the future:  future.some_attribute
        or getattr(future, name, default)

        :param name: attribute name
        :param default: a value to be used if no attribute is found
        :return:
        """
        if name[0] == '_':
            raise AttributeError(name)

        return getattr(self.result, name, default)

    def __getstate__(self):
        """
        used for getting object state to serialize when pickling
        :return: object
        """
        return self.result

    def __setstate__(self, state):
        """
        used for restoring object state when pickling
        :param state: object
        :return: None
        """
        self._result = state

    # this helps with duck-typing.
    # when grabbing the property for json encoder,
    # we can look for this unique attribute which is an alias for result
    # and we can be reasonably sure it is not accidentally grabbing
    # some other type of object.
    _redpipe_future_result = result


def _json_default_encoder(func):
    """
    Monkey-Patch the core json encoder library.
    This isn't as bad as it sounds.
    We override the default method so that if an object
    falls through and can't be encoded normally, we see if it is
    a Future object and return the result to be encoded.

    I set a special attribute on the Future object so I can tell
    that's what it is, and can grab the result.

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
            return o._redpipe_future_result  # noqa
        except AttributeError:
            pass
        return func(self, o)

    return inner


JSONEncoder.default = _json_default_encoder(JSONEncoder.default)
