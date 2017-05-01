# -*- coding: utf-8 -*-
"""
When sending commands to multiple redis backends in one redpipe.pipeline,
this module gives us an api to allow threaded async communication to those
different backends, improving parallelism.
"""
import sys
from six import reraise
import threading

__all__ = ['enable_threads', 'disable_threads']


class SynchronousTask(object):
    """
    This is the default for now.
    Just iterate through each backend sequentially.
    Slow but reliable.
    I'll make this a fallback once I feel confident in threaded behavior.
    """
    def __init__(self, target, args=(), kwargs=None):
        self._target = target
        self._args = args
        self._kwargs = kwargs
        self._exc_info = None
        self._result = None

    def run(self):
        # noinspection PyBroadException
        try:
            self._result = self._target(*self._args, **self._kwargs)
        except Exception:
            self._exc_info = sys.exc_info()
        finally:
            # Avoid a refcycle if the thread is running a function with
            # an argument that has a member that points to the thread.
            del self._target, self._args, self._kwargs

    start = run

    @property
    def result(self):
        if self._exc_info is not None:
            reraise(*self._exc_info)

        return self._result


class AsynchronousTask(threading.Thread):
    """
    use threads to talk to multiple redis backends simulaneously.
    Should decrease latency for the case when sending commands to multiple
    redis backends in one `redpipe.pipeline`.
    """
    def __init__(self, target, args=None, kwargs=None):
        super(AsynchronousTask, self).__init__()
        if args is None:
            args = ()
        if kwargs is None:
            kwargs = {}
        self._target = target
        self._args = args
        self._kwargs = kwargs
        self._exc_info = None
        self._result = None

    def run(self):
        # noinspection PyBroadException
        try:
            self._result = self._target(*self._args, **self._kwargs)
        except Exception:
            self._exc_info = sys.exc_info()
        finally:
            # Avoid a refcycle if the thread is running a function with
            # an argument that has a member that points to the thread.
            del self._target, self._args, self._kwargs

    @property
    def result(self):
        if self.is_alive():
            self.join()
        if self._exc_info is not None:
            reraise(*self._exc_info)

        return self._result


class TaskManager(object):
    """
    standardized interface for processing async vs synchronous tasks.
    """
    task = SynchronousTask

    @classmethod
    def set_task_type(cls, task):
        cls.task = task

    @classmethod
    def promise(cls, fn, *args, **kwargs):
        task = cls.task(target=fn, args=args, kwargs=kwargs)
        task.start()
        return task

    @classmethod
    def wait(cls, *futures):
        return [f.result for f in futures]


def enable_threads():
    """
    used to enable threaded behavior when talking to multiple redis backends
    in one pipeline execute call.
    Otherwise we don't need it.
    :return: None
    """
    TaskManager.set_task_type(SynchronousTask)


def disable_threads():
    """
    used to disable threaded behavior when talking to multiple redis backends
    in one pipeline execute call.
    Use this option if you are really concerned about python threaded behavior
    in your application.
    Doesn't apply if you are only ever talking to one redis backend at a time.
    :return: None
    """
    TaskManager.set_task_type(AsynchronousTask)


promise = TaskManager.promise
wait = TaskManager.wait
