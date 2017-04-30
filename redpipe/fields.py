import json
import re
from .compat import long, unicode
from .exceptions import InvalidFieldValue

__all__ = [
    'IntegerField',
    'FloatField',
    'TextField',
    'AsciiField',
    'BooleanField',
    'ListField',
    'DictField',
    'StringListField',
]


class BooleanField(object):
    """
    Used for boolean fields.
    """

    @classmethod
    def encode(cls, value):
        """
        convert a boolean value into something we can persist to redis.
        An empty string is the representation for False.
        :param obj:
        :return:
        """
        if value not in [True, False]:
            raise InvalidFieldValue('not a boolean')

        return '1' if value else ''

    @classmethod
    def decode(cls, obj):
        """
        convert from redis bytes into a boolean value
        :param obj:
        :return:
        """
        return bool(obj)


class FloatField(object):
    """
    Numeric field that supports integers and floats (values are turned into
    floats on load from persistence).
    """
    allowed = (float, int, long)

    @classmethod
    def decode(cls, value):
        return float(value)

    @classmethod
    def encode(cls, value):
        try:
            if float(value) + 0 == value:
                return repr(value)
        except (TypeError, ValueError):
            pass

        raise InvalidFieldValue('not a float')


class IntegerField(object):
    """
    Used for integer numeric fields.
    """

    @classmethod
    def decode(cls, value):
        return int(value)

    @classmethod
    def encode(cls, value):
        try:
            if int(value) + 0 == value:
                return repr(value)

        except (TypeError, ValueError):
            pass

        raise InvalidFieldValue('not an int')


class TextField(object):
    """
    A unicode string field.

    Encoded via utf-8 before writing to persistence.
    """
    _encoding = 'utf-8'

    @classmethod
    def encode(cls, value):
        if unicode(value) == value:
            return value.encode(cls._encoding)

        raise InvalidFieldValue('not text')

    @classmethod
    def decode(cls, value):
        return unicode(value.decode(cls._encoding))


class AsciiField(TextField):
    PATTERN = re.compile('^([ -~]+)?$')

    @classmethod
    def encode(cls, value):
        try:
            if cls.PATTERN.match(value):
                return value.encode(cls._encoding)
        except (TypeError, UnicodeError):
            pass

        raise InvalidFieldValue('not text')


class ListField(object):
    _encoding = 'utf-8'

    @classmethod
    def encode(cls, value):
        try:
            if list(value) == value:
                return json.dumps(value).encode(cls._encoding)
        except TypeError:
            pass

        raise InvalidFieldValue('not a list')

    @classmethod
    def decode(cls, value):
        try:
            return list(json.loads(value.decode(cls._encoding)))
        except (TypeError, AttributeError):
            return list(value)


class DictField(object):
    _encoding = 'utf-8'

    @classmethod
    def encode(cls, value):
        try:
            if dict(value) == value:
                return json.dumps(value).encode(cls._encoding)
        except (TypeError, ValueError):
            pass
        raise InvalidFieldValue('not a dict')

    @classmethod
    def decode(cls, value):
        try:
            return dict(json.loads(value.decode(cls._encoding)))
        except (TypeError, AttributeError):
            return dict(value)


class StringListField(object):
    _encoding = 'utf-8'

    @classmethod
    def decode(cls, value):
        try:
            data = [v for v in value.decode(cls._encoding).split(',') if
                    v != '']
            return data if data else None
        except AttributeError:
            return value

    @classmethod
    def encode(cls, value):

        try:
            if [str(v) for v in value] == value:
                return ",".join(value).encode(cls._encoding) if len(
                    value) > 0 else None
        except TypeError:
            pass

        raise InvalidFieldValue('not a list of strings')
