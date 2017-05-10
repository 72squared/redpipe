# -*- coding: utf-8 -*-
"""
When sending commands to multiple redis backends in one redpipe.pipeline,
this module gives us an api to allow threaded async communication to those
different backends, improving parallelism.

The AsynchronousTask is well tested and should work well.
But if you see any issues, you can easily disable this in your application.

.. code-block:: python

    redpipe.disable_threads()

Please report any `issues <https://github.com/72squared/redpipe/issues>`_.
"""
import sys
from six import reraise
import threading

__all__ = ['enable_threads', 'disable_threads']


class SynchronousTask(object):
    """
    Iterate through each backend sequentially.
    Fallback method if you aren't comfortable with threads.
    """

    def __init__(self, target, args=None, kwargs=None):
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
    task = AsynchronousTask

    @classmethod
    def set_task_type(cls, task):
        cls.task = task

    @classmethod
    def promise(cls, fn, *args, **kwargs):
        """
        Used to build a task based on a callable function and the arguments.
        Kick it off and start execution of the task.

        :param fn: callable
        :param args: tuple
        :param kwargs: dict
        :return: SynchronousTask or AsynchronousTask
        """
        task = cls.task(target=fn, args=args, kwargs=kwargs)
        task.start()
        return task

    @classmethod
    def wait(cls, *tasks):
        """
        Wait for all tasks to finish completion.

        :param tasks: tulple of tasks, AsynchronousTask or SynchronousTask.
        :return: list of the results from each task.
        """
        return [f.result for f in tasks]


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
