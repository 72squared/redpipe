class Error(Exception):
    """Base class for all ORM-related errors"""


class UniqueKeyViolation(Error):
    """Raised when trying to save an entity without a distinct column value"""


class InvalidOperation(Error):
    """
    Raised when trying to delete or modify a column that
    cannot be deleted or modified
     """


class QueryError(InvalidOperation):
    """
    Raised when arguments to ``Model.get_by()``
    or ``Query.filter`` are not valid
    """


class FieldError(Error):
    """Raised when your field definitions are not kosher"""


class MissingField(FieldError):
    """
    Raised when a model has a required field,
    but it is not provided on construction
    """


class InvalidFieldValue(FieldError):
    """
    Raised when you attempt to pass a primary key on entity creation or when
    data assigned to a field is the wrong type
    """


class LockException(Exception):
    """Raised when unable to obtain a lock for a key"""


class OperationUnsupportedException(Exception):
    pass
