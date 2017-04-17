__all__ = [
    'Error',
    'InvalidOperation',
    'InvalidFieldValue',
    'FieldError',
    'AlreadyConnected',
]


class Error(Exception):
    """Base class for all ORM-related errors"""


class InvalidOperation(Error):
    """
    Raised when trying to delete or modify a column that
    cannot be deleted or modified
     """


class FieldError(Error):
    """Raised when your field definitions are not kosher"""


class InvalidFieldValue(FieldError):
    """
    Raised when you attempt to pass a primary key on entity creation or when
    data assigned to a field is the wrong type
    """


class AlreadyConnected(Error):
    """
    raised when you try to connect and change the ORM connection
    without explicitly disconnecting first.
    """
