# -*- coding: utf-8 -*-

"""
A module for marshalling data in and out of redis and back into the python
data type we expect.

Used extensively in the `redpipe.keyspaces` module for type-casting keys and
values.

"""

import json
import re
import six
from .exceptions import InvalidValue

__all__ = [
    'IntegerField',
    'FloatField',
    'TextField',
    'AsciiField',
    'BinaryField',
    'BooleanField',
    'ListField',
    'DictField',
    'StringListField',
]

unicode = unicode if six.PY2 else str  # noqa


class BooleanField(object):
    """
    Used for boolean fields.
    """

    @classmethod
    def encode(cls, value):
        """
        convert a boolean value into something we can persist to redis.
        An empty string is the representation for False.

        :param value: bool
        :return: bytes
        """
        if value not in [True, False]:
            raise InvalidValue('not a boolean')

        return b'1' if value else b''

    @classmethod
    def decode(cls, value):
        """
        convert from redis bytes into a boolean value

        :param value: bytes
        :return: bool
        """
        return None if value is None else bool(value)


class FloatField(object):
    """
    Numeric field that supports integers and floats (values are turned into
    floats on load from persistence).
    """

    @classmethod
    def decode(cls, value):
        """
        decode the bytes from redis back into a float

        :param value: bytes
        :return: float
        """
        return None if value is None else float(value)

    @classmethod
    def encode(cls, value):
        """
        encode a floating point number to bytes in redis

        :param value: float
        :return: bytes
        """
        try:
            if float(value) + 0 == value:
                return repr(value)
        except (TypeError, ValueError):
            pass

        raise InvalidValue('not a float')


class IntegerField(object):
    """
    Used for integer numeric fields.
    """

    @classmethod
    def decode(cls, value):
        """
        read bytes from redis and turn it back into an integer.

        :param value: bytes
        :return: int
        """
        return None if value is None else int(value)

    @classmethod
    def encode(cls, value):
        """
        take an integer and turn it into a string representation
        to write into redis.

        :param value: int
        :return: str
        """
        try:
            coerced = int(value)
            if coerced + 0 == value:
                return repr(coerced)

        except (TypeError, ValueError):
            pass

        raise InvalidValue('not an int')


class TextField(object):
    """
    A unicode string field.

    Encoded via utf-8 before writing to persistence.
    """
    _encoding = 'utf-8'

    @classmethod
    def encode(cls, value):
        """
        take a valid unicode string and turn it into utf-8 bytes

        :param value: unicode, str
        :return: bytes
        """
        coerced = unicode(value)
        if coerced == value:
            return coerced.encode(cls._encoding)

        raise InvalidValue('not text')

    @classmethod
    def decode(cls, value):
        """
        take bytes from redis and turn them into unicode string

        :param value:
        :return:
        """
        return None if value is None else unicode(value.decode(cls._encoding))


class AsciiField(TextField):
    """
    Used for ascii-only text
    """
    PATTERN = re.compile('^([ -~]+)?$')

    @classmethod
    def encode(cls, value):
        """
        take a list of strings and turn it into utf-8 byte-string

        :param value:
        :return:
        """
        coerced = unicode(value)
        if coerced == value and cls.PATTERN.match(coerced):
            return coerced.encode(cls._encoding)

        raise InvalidValue('not ascii')


class BinaryField(object):
    """
    A bytes field. Not encoded.
    """

    @classmethod
    def encode(cls, value):
        """
        write binary data into redis without encoding it.

        :param value: bytes
        :return: bytes
        """
        try:
            coerced = bytes(value)
            if coerced == value:
                return coerced
        except (TypeError, UnicodeError):
            pass

        raise InvalidValue('not binary')

    @classmethod
    def decode(cls, value):
        """
        read binary data from redis and pass it on through.

        :param value: bytes
        :return: bytes
        """
        return None if value is None else bytes(value)


class ListField(object):
    """
    A list field. Marshalled in and out of redis via json.
    Values of the list can be any arbitrary data.
    """
    _encoding = 'utf-8'

    @classmethod
    def encode(cls, value):
        """
        take a list and turn it into a utf-8 encoded byte-string for redis.

        :param value: list
        :return: bytes
        """
        try:
            coerced = list(value)
            if coerced == value:
                return json.dumps(coerced).encode(cls._encoding)
        except TypeError:
            pass

        raise InvalidValue('not a list')

    @classmethod
    def decode(cls, value):
        """
        take a utf-8 encoded byte-string from redis and
        turn it back into a list

        :param value: bytes
        :return: list
        """
        try:
            return None if value is None else \
                list(json.loads(value.decode(cls._encoding)))
        except (TypeError, AttributeError):
            return list(value)


class DictField(object):
    _encoding = 'utf-8'

    @classmethod
    def encode(cls, value):
        """
        encode the dict as a json string to be written into redis.

        :param value: dict
        :return: bytes
        """
        try:
            coerced = dict(value)
            if coerced == value:
                return json.dumps(coerced).encode(cls._encoding)
        except (TypeError, ValueError):
            pass
        raise InvalidValue('not a dict')

    @classmethod
    def decode(cls, value):
        """
        decode the data from a json string in redis back into a dict object.

        :param value: bytes
        :return: dict
        """
        try:
            return None if value is None else \
                dict(json.loads(value.decode(cls._encoding)))
        except (TypeError, AttributeError):
            return dict(value)


class StringListField(object):
    """
    Used for storing a list of strings, serialized as a comma-separated list.
    """
    _encoding = 'utf-8'

    @classmethod
    def decode(cls, value):
        """
        decode the data from redis.
        :param value: bytes
        :return: list
        """
        try:
            data = [v for v in value.decode(cls._encoding).split(',') if
                    v != '']
            return data if data else None
        except AttributeError:
            return value

    @classmethod
    def encode(cls, value):
        """
        the list it so it can be stored in redis.

        :param value: list
        :return: bytes
        """
        try:
            coerced = [str(v) for v in value]
            if coerced == value:
                return ",".join(coerced).encode(cls._encoding) if len(
                    value) > 0 else None
        except TypeError:
            pass

        raise InvalidValue('not a list of strings')
