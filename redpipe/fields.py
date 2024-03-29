# -*- coding: utf-8 -*-

"""
A module for marshalling data in and out of redis and back into the python
data type we expect.

Used extensively in the `redpipe.keyspaces` module for type-casting keys and
values.

"""

import json
import re
import typing

# python 3.7 compatibility change
from typing import (TypeVar, Generic, Optional, Union)
try:
    from typing import Protocol
except ImportError:
    from typing_extensions import Protocol  # type: ignore


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
    'Field'
]

T = TypeVar('T')


class Field(Protocol, Generic[T]):
    @classmethod
    def encode(cls, value: T) -> bytes: ...

    @classmethod
    def decode(cls, value: Optional[bytes]) -> Optional[T]: ...


class BooleanField(object):
    """
    Used for boolean fields.
    """
    @classmethod
    def is_true(cls, val):
        if val is True:
            return True
        if val is False:
            return False

        strval = str(val).lower()
        if strval in ['true', '1']:
            return True

        if strval in ['false', '0', 'none', '']:
            return False

        return True if val else False

    @classmethod
    def encode(cls, value: bool) -> bytes:
        """
        convert a boolean value into something we can persist to redis.
        An empty string is the representation for False.

        :param value: bool
        :return: bytes
        """
        return b'1' if cls.is_true(value) else b''

    @classmethod
    def decode(cls, value: Optional[bytes]) -> Optional[bool]:
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
    def decode(cls, value: Optional[bytes]) -> Optional[float]:
        """
        decode the bytes from redis back into a float

        :param value: bytes
        :return: float
        """
        return None if value is None else float(value)

    @classmethod
    def encode(cls, value: float) -> bytes:
        """
        encode a floating point number to bytes in redis

        :param value: float
        :return: bytes
        """
        try:
            coerced = float(value)
        except (TypeError, ValueError):
            raise InvalidValue('not a float')
        response = repr(coerced)
        if response.endswith('.0'):
            response = response[:-2]
        return response.encode()


class IntegerField(object):
    """
    Used for integer numeric fields.
    """

    @classmethod
    def decode(cls, value: Optional[bytes]) -> Optional[int]:
        """
        read bytes from redis and turn it back into an integer.

        :param value: bytes
        :return: int
        """
        return None if value is None else int(float(value))

    @classmethod
    def encode(cls, value: int) -> bytes:
        """
        take an integer and turn it into a string representation
        to write into redis.

        :param value: int
        :return: str
        """
        try:
            return repr(int(float(value))).encode()
        except (TypeError, ValueError):
            raise InvalidValue('not an int')


class TextField(object):
    """
    A unicode string field.

    Encoded via utf-8 before writing to persistence.
    """
    _encoding = 'utf-8'

    @classmethod
    def encode(cls, value: str) -> bytes:
        """
        take a valid unicode string and turn it into utf-8 bytes

        :param value: unicode, str
        :return: bytes
        """
        coerced = str(value)
        if coerced == value:
            return coerced.encode(cls._encoding)

        raise InvalidValue('not text')

    @classmethod
    def decode(cls, value: Optional[bytes]) -> Optional[str]:
        """
        take bytes from redis and turn them into unicode string

        :param value:
        :return:
        """
        return None if value is None else str(value.decode(cls._encoding))


class AsciiField(TextField):
    """
    Used for ascii-only text
    """
    PATTERN = re.compile('^([ -~]+)?$')

    @classmethod
    def encode(cls, value: str) -> bytes:
        """
        take a list of strings and turn it into utf-8 byte-string

        :param value:
        :return:
        """
        coerced = str(value)
        if coerced == value and cls.PATTERN.match(coerced):
            return coerced.encode(cls._encoding)

        raise InvalidValue('not ascii')


class BinaryField(object):
    """
    A bytes field. Not encoded.
    """

    @classmethod
    def encode(cls, value: bytes) -> bytes:
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
    def decode(cls, value: Optional[bytes]) -> Optional[bytes]:
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
    def encode(cls, value: list) -> bytes:
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
    def decode(cls, value: Union[bytes, None, list]) -> Optional[list]:
        """
        take a utf-8 encoded byte-string from redis and
        turn it back into a list

        :param value: bytes
        :return: list
        """
        try:
            return None if value is None else \
                list(json.loads(value.decode(cls._encoding)))  # type: ignore
        except (TypeError, AttributeError):
            return list(value)  # type: ignore


class DictField(object):
    _encoding = 'utf-8'

    @classmethod
    def encode(cls, value: dict) -> bytes:
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
    def decode(cls, value: Union[bytes, None, dict]) -> Optional[dict]:
        """
        decode the data from a json string in redis back into a dict object.

        :param value: bytes
        :return: dict
        """
        try:
            return None if value is None else \
                dict(json.loads(value.decode(cls._encoding)))  # type: ignore
        except (TypeError, AttributeError):
            return dict(value)  # type: ignore


class StringListField(object):
    """
    Used for storing a list of strings, serialized as a comma-separated list.
    """
    _encoding = 'utf-8'

    @classmethod
    def decode(cls,
               value: Union[bytes, None, typing.List[str]]
               ) -> Optional[typing.List[str]]:
        """
        decode the data from redis.
        :param value: bytes
        :return: list
        """
        if value is None or isinstance(value, list):
            return value

        try:
            data = [v for v in value.decode(cls._encoding).split(',') if
                    v != '']
            return data if data else None
        except AttributeError:
            return None

    @classmethod
    def encode(cls, value: typing.List[str]) -> bytes:
        """
        encode the list it so it can be stored in redis.

        :param value: list
        :return: bytes
        """
        try:
            coerced = [str(v) for v in value]
            if coerced == value:
                return ",".join(coerced).encode(cls._encoding) if len(
                    value) > 0 else b''
        except TypeError:
            pass

        raise InvalidValue('not a list of strings')
