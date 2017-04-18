__all__ = [
    'Error',
    'ResultNotReady',
    'InvalidOperation',
    'InvalidFieldValue',
    'FieldError',
    'AlreadyConnected',
    'InvalidPipeline'
]


class Error(Exception):
    """Base class for all redpipe errors"""


class ResultNotReady(Error):
    """
    Raised when you access a data from a DeferredResult before it is assigned.
    """


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


class NotConfigured(Error):
    """
    raised when you try to use a connection that isn't configured.
    """


class InvalidPipeline(Error):
    """
    raised when you pass in a pipeline into context that doesn't
    have the same name.
    """
