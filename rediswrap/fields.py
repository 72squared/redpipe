from .compat import long, unicode, basestring

__all__ = [
    'Field',
    'IntegerField',
    'FloatField',
    'TextField',
    'BooleanField',
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
        return '1' if obj else ''

    @classmethod
    def from_persistence(cls, obj):
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
        return value.encode('utf-8')

    @classmethod
    def from_persistence(cls, value):
        return value.decode('utf-8')
