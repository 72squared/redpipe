# -*- coding: utf-8 -*-

#      (                     (
#      )\ )           (      )\ )
#     (()/(     (     )\ )  (()/(   (              (
#      /(_))   ))\   (()/(   /(_))  )\   `  )     ))\
#     (_))    /((_)   ((_)) (_))   ((_)  /(/(    /((_)
#     | _ \   ())     _| |  | _ \   (_) ((_)_   (_))
#     |   /  / -_)  / _` |  |  _/   | | | '_ \  / -_)
#     |_|_\  \___|  \__,_|  |_|     |_| | .__/  \___|
#                                       |_|
#

"""
Redpipe makes redis pipelines easier to use in python.

Usage:

.. code:: python

    import redpipe
    import redis

    redpipe.connect_redis(redis.Redis())
    with redpipe.pipeline() as pipe:
        foo = pipe.incr('foo')
        bar = pipe.incr('bar)
        pipe.execute()
    print([foo, bar])


Module Structure
----------------

This is the structure of the top level of the package, grouped by category.

Connections
-----------
* connect_redis
* disconnect
* reset
* pipeline
* autoexec

Fields
------
* IntegerField
* FloatField
* TextField
* AsciiField
* BinaryField
* BooleanField
* ListField
* DictField',
* StringListField

Keyspaces
---------
* String
* Set
* List
* SortedSet
* Hash
* HyperLogLog

Exceptions
----------
* Error
* ResultNotReady
* InvalidOperation
* InvalidValue
* AlreadyConnected
* InvalidPipeline

Misc
----
* Future
* Struct
* enable_threads
* disable_threads


You shouldn't need to import the submodules directly.
"""
from .version import __version__  # noqa
from .pipelines import *  # noqa
from .fields import *  # noqa
from .connections import *  # noqa
from .structs import *  # noqa
from .keyspaces import *  # noqa
from .exceptions import *  # noqa
from .futures import *  # noqa
from .tasks import *  # noqa
