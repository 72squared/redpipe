import json
import re
from .compat import long, unicode, basestring

__all__ = [
    'Field',
    'IntegerField',
    'FloatField',
    'TextField',
    'AsciiField',
    'BooleanField',
    'JsonField',
    'ListField',
    'DictField',
    'StringListField',
]


class Field(object):
    """
    Field objects handle data conversion to/from strings in redis
    """
    allowed = ()

    __slots__ = []

    @classmethod
    def from_persistence(cls, value):
        convert = cls.allowed[0] if \
            isinstance(cls.allowed, (tuple, list)) else cls.allowed
        return convert(value)

    @classmethod
    def to_persistence(cls, value):
        return repr(value)

    @classmethod
    def validate(cls, value):
        return value is None or isinstance(value, cls.allowed)


class BooleanField(Field):
    """
    Used for boolean fields.
    """
    allowed = bool

    @classmethod
    def to_persistence(cls, obj):
        """
        convert a boolean value into something we can persist to redis.
        An empty string is the representation for False.
        :param obj:
        :return:
        """
        return '1' if obj else ''

    @classmethod
    def from_persistence(cls, obj):
        """
        convert from redis bytes into a boolean value
        :param obj:
        :return:
        """
        return bool(obj)


class FloatField(Field):
    """
    Numeric field that supports integers and floats (values are turned into
    floats on load from persistence).
    """
    allowed = (float, int, long)


class IntegerField(Field):
    """
    Used for integer numeric fields.
    """
    allowed = (int, long)


class TextField(Field):
    """
    A unicode string field.

    Encoded via utf-8 before writing to persistence.
    """
    allowed = (unicode, str, basestring)

    @classmethod
    def to_persistence(cls, value):
        """
        serialize utf-8 character string into bytes for redis to write.
        :param value:
        :return: utf-8 encoded bytes
        """
        return value

    @classmethod
    def from_persistence(cls, value):
        """
        take bytes returned from redis and convert them into
        unicode safe string.
        :param value:
        :return: utf-8 decoded string
        """
        return value


class AsciiField(TextField):
    PATTERN = re.compile('^([ -~]+)?$')

    @classmethod
    def validate(cls, value):
        if not super(AsciiField, cls).validate(value):
            return False

        return True if cls.PATTERN.match(value) else False


class JsonField(Field):
    """
    Allows for more complicated nested structures as attributes.
    """
    allowed = (dict, list)

    @classmethod
    def to_persistence(cls, value):
        return json.dumps(value)

    @classmethod
    def from_persistence(cls, value):
        if isinstance(value, cls.allowed):
            return value
        return json.loads(value)


class ListField(JsonField):
    allowed = list


class DictField(JsonField):
    allowed = dict


class StringListField(Field):
    allowed = list

    @classmethod
    def from_persistence(cls, value):
        if isinstance(value, cls.allowed):
            return value

        if len(value) > 0:
            return value.split(',')
        else:
            return None

    @classmethod
    def to_persistence(cls, value):
        return ",".join(value) if len(value) > 0 else None

    @classmethod
    def validate(cls, value):
        if value is None:
            return True

        if not isinstance(value, cls.allowed):
            return False

        for v in value:
            if not isinstance(v, (unicode, str, basestring)):
                return False
        return True
