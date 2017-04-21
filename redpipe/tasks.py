import sys
from six import reraise
# import threading


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

#
# class AsynchronousTask(threading.Thread):
#
#     def __init__(self, target, args=None, kwargs=None):
#         super(AsynchronousTask, self).__init__()
#         if args is None:
#             args = ()
#         if kwargs is None:
#             kwargs = {}
#         self._target = target
#         self._args = args
#         self._kwargs = kwargs
#         self._exc_info = None
#         self._result = None
#
#     def start(self):
#         self.run()
#
#     def run(self):
#         # noinspection PyBroadException
#         try:
#             self._result = self._target(*self._args, **self._kwargs)
#         except Exception:
#             self._exc_info = sys.exc_info()
#         finally:
#             # Avoid a refcycle if the thread is running a function with
#             # an argument that has a member that points to the thread.
#             del self._target, self._args, self._kwargs
#
#     @property
#     def result(self):
#         if self.is_alive():
#             self.join()
#         if self._exc_info is not None:
#             reraise(*self._exc_info)
#
#         return self._result

Task = SynchronousTask


def promise(fn, *args, **kwargs):

    task = Task(target=fn, args=args, kwargs=kwargs)
    task.start()
    return task


def wait(*futures):
    return [f.result for f in futures]
