# -*- coding: utf-8 -*-
"""
This module contains the set of all of redpipe exceptions.
"""

__all__ = [
    'Error',
    'ResultNotReady',
    'InvalidOperation',
    'InvalidValue',
    'AlreadyConnected',
    'InvalidPipeline'
]


class Error(Exception):
    """
    Base class for all redpipe errors
    """


class ResultNotReady(Error):
    """
    Raised when you access a data from a Future before it is assigned.
    """


class InvalidOperation(Error):
    """
    Raised when trying to perform an operation disallowed by the redpipe api.
    """


class InvalidValue(Error):
    """
    Raised when data assigned to a field is the wrong type
    """


class AlreadyConnected(Error):
    """
    raised when you try to connect and change the ORM connection
    without explicitly disconnecting first.
    """


class InvalidPipeline(Error):
    """
    raised when you try to use a pipeline that isn't configured correctly.
    """
