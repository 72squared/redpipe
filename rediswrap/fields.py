import json
from decimal import Decimal
from .exceptions import InvalidFieldValue  # noqa

__all__ = '''
Field
IntegerField
FloatField
DecimalField
StringField
StringListField
TextField
JsonField
ListField
BooleanField
'''.split()

_NUMERIC = (0, 0.0, Decimal('0'))

NULL = object()

try:
    INT_TYPES = (int, long)
    FLOAT_TYPES = (int, long, float)
    UNICODE_TYPES = (unicode, str)
except NameError:
    INT_TYPES = (int)
    FLOAT_TYPES = (int, float)
    UNICODE_TYPES = (str)


class Field(object):
    """
    Field objects handle data conversion to/from strings, store metadata
    about indices, etc. Note that these are "heavy" fields, in that whenever
    data is read/written, it must go through descriptor processing. This is
    primarily so that (for example) if you try to write a Decimal to a Float
    field, you get an error the moment you try to do it, not some time later
    when you try to save the object (though saving can still cause an error
    during the conversion process).

    Standard Arguments:

        * *required* - determines whether this field is required on
          creation
        * *default* - a default value (either a callable or a simple value)
          when this field is not provided
        * *unique* - can only be enabled on ``String`` fields, allows for
          required distinct field values (like an email address on a User
          model)

    Notes:

        * Fields with 'unique' set to True can only be string fields
        * You can only have one unique field on any model
        * If you set required to True, then you must have the field set
          during object construction: ``MyModel(col=val)``
    """
    _allowed = ()

    __slots__ = 'default'.split()

    def __init__(self, default=NULL):
        self.default = default

    def from_persistence(self, value):
        convert = self._allowed[0] if \
            isinstance(self._allowed, (tuple, list)) else self._allowed
        return convert(value)

    def to_persistence(self, value):
        if isinstance(value, INT_TYPES):
            return str(value)
        return repr(value)

    def validate(self, value):
        if value is not None:
            if isinstance(value, self._allowed):
                return


class BooleanField(Field):
    """
    Used for boolean fields.

    All standard arguments supported.

    All values passed in on creation are casted via bool().
    Originally thought we would want to distinguish between None and False
    cases, but it actually gets messy and I think it is better as an on/off
    switch.

    Used via::

        class MyModel(Model):
            col = Boolean()

    Queries via ``MyModel.get_by(...)`` and ``MyModel.query.filter(...)`` work
    as expected when passed ``True`` or ``False``.

    .. note: these fields are not sortable by default.
    """
    _allowed = bool

    def __init__(self):
        super(BooleanField, self).__init__(default=False)

    def to_persistence(self, obj):
        return '1' if obj else None

    def from_persistence(self, obj):
        return bool(obj)


class DecimalField(Field):
    """
    A Decimal-only numeric field (converts ints/longs into Decimals
    automatically). Attempts to assign Python float will fail.

    All standard arguments supported.

    Used via::

        class MyModel(Model):
            col = Decimal()
    """
    _allowed = Decimal

    def __init__(self, required=False, default=NULL):
        """
        don't allow as primary key.
        rounding errors.
        Args:
            required:
            default:
        """
        super(DecimalField, self).__init__(required=required, default=default)

    def from_persistence(self, value):
        return Decimal(value)

    def to_persistence(self, value):
        return str(value)


class FloatField(Field):
    """
    Numeric field that supports integers and floats (values are turned into
    floats on load from persistence).

    All standard arguments supported.

    Used via::

        class MyModel(Model):
            col = Float()
    """
    _allowed = FLOAT_TYPES


class IntegerField(Field):
    """
    Used for integer numeric fields.

    All standard arguments supported.

    Used via::

        class MyModel(Model):
            col = Integer()
    """
    _allowed = INT_TYPES


class JsonField(Field):
    """
    Allows for more complicated nested structures as attributes.

    Used via::

        class MyModel(Model):
            col = Json()
    """
    _allowed = (dict, list, tuple, set)

    def to_persistence(self, value):
        return json.dumps(value)

    def from_persistence(self, value):
        if isinstance(value, self._allowed):
            return value
        return json.loads(value)


class ListField(JsonField):
    _allowed = list

    def from_persistence(self, value):
        if isinstance(value, self._allowed):
            return value
        try:
            return json.loads(value)
        except (ValueError, TypeError) as e:
            if isinstance(value, str):
                if len(value) > 0:
                    return value.split(',')
                else:
                    return None
            raise InvalidFieldValue(*e.args)


class StringListField(ListField):
    _allowed = list

    def to_persistence(self, value):
        return ",".join(value) if len(value) > 0 else None


class StringField(Field):
    """
    A plain string field. Trying to save unicode strings will probably result
    in an error, if not bad data.

    All standard arguments supported.

    Used via::

        class MyModel(Model):
            col = String()
    """
    _allowed = str

    def from_persistence(self, value):
        return str(value)

    def to_persistence(self, value):
        return value


class TextField(Field):
    """
    A unicode string field.

    All standard arguments supported, except for ``unique``.

    Aside from not supporting ``unique`` indices, will generally have the same
    behavior as a ``String`` field, only supporting unicode strings. Data is
    encoded via utf-8 before writing to persistence. If you would like to
    create your own field to encode/decode differently, examine the source
    find out how to do it.

    Used via::

        class MyModel(Model):
            col = Text()
    """
    _allowed = UNICODE_TYPES

    def to_persistence(self, value):
        return value.encode('utf-8')

    def from_persistence(self, value):
        if isinstance(value, str):
            return value.decode('utf-8')
        return value
