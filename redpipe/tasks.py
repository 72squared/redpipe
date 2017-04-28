import sys
from six import reraise
import threading

__all__ = ['use_asyncronous_tasks', 'use_syncronous_tasks']


class SynchronousTask(object):
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


def use_asyncronous_tasks():
    TaskManager.set_task_type(SynchronousTask)


def use_syncronous_tasks():
    TaskManager.set_task_type(AsynchronousTask)


promise = TaskManager.promise
wait = TaskManager.wait
