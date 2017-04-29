import json
import re
from .compat import long, unicode

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
    def to_redis(cls, obj):
        """
        convert a boolean value into something we can persist to redis.
        An empty string is the representation for False.
        :param obj:
        :return:
        """
        return '1' if obj else ''

    @classmethod
    def from_redis(cls, obj):
        """
        convert from redis bytes into a boolean value
        :param obj:
        :return:
        """
        return bool(obj)

    @classmethod
    def validate(cls, value):
        return True if value in [None, True, False] else False


class FloatField(object):
    """
    Numeric field that supports integers and floats (values are turned into
    floats on load from persistence).
    """
    allowed = (float, int, long)

    @classmethod
    def from_redis(cls, value):
        return float(value)

    @classmethod
    def to_redis(cls, value):
        return repr(value)

    @classmethod
    def validate(cls, value):
        try:
            return True if float(value) + 0 == value else False
        except (TypeError, ValueError):
            return False


class IntegerField(object):
    """
    Used for integer numeric fields.
    """

    @classmethod
    def validate(cls, value):
        try:
            return True if int(value) + 0 == value else False
        except (TypeError, ValueError):
            return False

    @classmethod
    def from_redis(cls, value):
        return int(value)

    @classmethod
    def to_redis(cls, value):
        return repr(value)


class TextField(object):
    """
    A unicode string field.

    Encoded via utf-8 before writing to persistence.
    """

    @classmethod
    def to_redis(cls, value):
        return value

    @classmethod
    def from_redis(cls, value):
        return value

    @classmethod
    def validate(cls, value):
        return unicode(value) == value


class AsciiField(TextField):
    PATTERN = re.compile('^([ -~]+)?$')

    @classmethod
    def validate(cls, value):
        try:
            return True if cls.PATTERN.match(value) else False
        except TypeError:
            return False


class ListField(object):
    @classmethod
    def to_redis(cls, value):
        return json.dumps(value)

    @classmethod
    def from_redis(cls, value):
        try:
            return list(json.loads(value))
        except TypeError:
            return list(value)

    @classmethod
    def validate(cls, value):
        try:
            return list(value) == value
        except TypeError:
            return False


class DictField(object):
    @classmethod
    def to_redis(cls, value):
        return json.dumps(value)

    @classmethod
    def from_redis(cls, value):
        try:
            return dict(json.loads(value))
        except TypeError:
            return dict(value)

    @classmethod
    def validate(cls, value):
        try:
            return dict(value) == value
        except (TypeError, ValueError):
            return False


class StringListField(object):
    @classmethod
    def from_redis(cls, value):
        try:
            data = [v for v in value.split(',') if v != '']
            return data if data else None
        except AttributeError:
            return value

    @classmethod
    def to_redis(cls, value):
        return ",".join(value) if len(value) > 0 else None

    @classmethod
    def validate(cls, value):
        try:
            return [str(v) for v in value] == value
        except TypeError:
            return False
