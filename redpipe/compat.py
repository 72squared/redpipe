"""Internal module for Python 2 backwards compatibility."""
import sys

if sys.version_info[0] < 3:  # noqa
    unicode = unicode  # noqa
    long = long  # noqa
    bytes = str  # noqa
    basestring = basestring  # noqa

else:
    unicode = str  # noqa
    safe_unicode = str
    long = int  # noqa
    bytes = bytes  # noqa
    basestring = str  # noqa
