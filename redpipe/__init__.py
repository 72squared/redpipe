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
RedPipe
=======

Redpipe makes redis pipelines easier to use in python.

Usage:

    import redpipe
    import redis

    redpipe.connect_redis(redis.Redis())
    with redpipe.pipeline() as pipe:
        foo = pipe.incr('foo')
        bar = pipe.incr('bar)
        pipe.execute()
    print([foo, bar])

For more information, see docs: http://redpipe.readthedocs.io/
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
