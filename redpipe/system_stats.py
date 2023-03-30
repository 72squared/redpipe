import threading
from os import getenv
from contextlib import contextmanager



ENABLE_REDPIPE_STATS = getenv('ENABLE_REDPIPE_STATS', 'false') == 'true'

threading_local = threading.local()
threading_local.futures_accessed = 0
threading_local.futures_created = 0
threading_local.futures_accessed_ids = []
@contextmanager
def log_redpipe_stats(name: str, logger, pid):
    """
    Resets futures created and accessed counts before a function is called and then measures those values to report after
    """
    if not ENABLE_REDPIPE_STATS:
        yield
    else:
        threading_local.futures_accessed = 0
        threading_local.futures_created = 0
        threading_local.futures_accessed_ids = []
        yield
        consumed = threading_local.futures_accessed / threading_local.futures_created \
            if threading_local.futures_created != 0 else 0
        logging_data = {
            "pid": pid,
            "name": name,
            "futures_accessed": threading_local.futures_accessed,
            "futures_created": threading_local.futures_created,
            "Consumption Percentage": "{:.0%}".format(consumed)
        }
        logger.info("REDPIPE_STATS", **logging_data)
