import pycassa_logger
import logging
import threading
import functools

def sync(lock_name):
    def wrapper(f):
        @functools.wraps(f)
        def wrapped(self, *args, **kwargs):
            lock = getattr(self, lock_name)
            try:
                lock.acquire()
                return f(self, *args, **kwargs)
            finally:
                lock.release()

        return wrapped

    return wrapper


class StatsLogger(object):
    """
    Basic stats logger that increment counts. You can plot these as `COUNTER` or
    `DERIVED` (RRD) or apply derivative (graphite) except for ``opened``, which tracks
    the currently opened connections.

    Usage::

        >>> pool = ConnectionPool(...)
        >>> stats_logger = StatsLogger()
        >>> pool.add_listener(stats_logger)
        >>>
        >>> # use the pool for a while...
        >>> import pprint
        >>> pprint.pprint(stats_logger.stats)
        {'at_max': 0,
         'checked_in': 401,
         'checked_out': 403,
         'created': {'failure': 0, 'success': 0},
         'disposed': {'failure': 0, 'success': 0},
         'failed': 1,
         'list': 0,
         'opened': {'current': 2, 'max': 2},
         'recycled': 0}


    Get your stats as ``stats_logger.stats`` and push them to your metrics
    system.
    """

    def __init__(self):
        #some callbacks are already locked by pool_lock, it's just simpler to have a global here for all operations
        self.lock = threading.Lock()
        self.reset()

    @sync('lock')
    def reset(self):
        """ Reset all counters to 0 """
        self._stats = {
            'created': {
                'success': 0,
                'failure': 0,
                },
            'checked_out': 0,
            'checked_in': 0,
            'opened': {
                'current': 0,
                'max': 0
            },
            'disposed': {
                'success': 0,
                'failure': 0
            },
            'recycled': 0,
            'failed': 0,
            'list': 0,
            'at_max': 0
        }


    def name_changed(self, new_logger):
        self.logger = new_logger

    @sync('lock')
    def connection_created(self, dic):
        level = pycassa_logger.levels[dic.get('level', 'info')]
        if level <= logging.INFO:
            self._stats['created']['success'] += 1
        else:
            self._stats['created']['failure'] += 1

    @sync('lock')
    def connection_checked_out(self, dic):
        self._stats['checked_out'] += 1
        self._update_opened(1)

    @sync('lock')
    def connection_checked_in(self, dic):
        self._stats['checked_in'] += 1
        self._update_opened(-1)

    def _update_opened(self, value):
        self._stats['opened']['current'] += value
        if self._stats['opened']['current'] > self._stats['opened']['max']:
            self._stats['opened']['max'] = self._stats['opened']['current']

    @sync('lock')
    def connection_disposed(self, dic):
        level = pycassa_logger.levels[dic.get('level', 'info')]
        if level <= logging.INFO:
            self._stats['disposed']['success'] += 1
        else:
            self._stats['disposed']['failure'] += 1

    @sync('lock')
    def connection_recycled(self, dic):
        self._stats['recycled'] += 1

    @sync('lock')
    def connection_failed(self, dic):
        self._stats['failed'] += 1

    @sync('lock')
    def obtained_server_list(self, dic):
        self._stats['list'] += 1

    @sync('lock')
    def pool_disposed(self, dic):
        pass

    @sync('lock')
    def pool_at_max(self, dic):
        self._stats['at_max'] += 1

    @property
    def stats(self):
        return self._stats
