""" Logging facilities for pycassa. """

import logging

__all__ = ['PycassaLogger']

levels = {'debug': logging.DEBUG,
          'info': logging.INFO,
          'warn': logging.WARN,
          'error': logging.ERROR,
          'critical': logging.CRITICAL}

_DEFAULT_LOGGER_NAME = 'pycassa'
_DEFAULT_LEVEL = 'info'

class PycassaLogger:
    """
    The root logger for pycassa.

    This uses a singleton-like pattern,
    so creating a new instance will always give you the
    same result. This means that you can adjust all of
    pycassa's logging by calling methods on any instance.

    pycassa does *not* automatically add a handler to the
    logger, so logs will not be captured by default. You
    *must* add a :class:`logging.Handler()` object to
    the root handler for logs to be captured.  See the
    example usage below.

    By default, the root logger name is 'pycassa' and the
    logging level is 'info'.

    The available levels are:

    * debug
    * info
    * warn
    * error
    * critical

    Example Usage::

        >>> import logging
        >>> log = pycassa.PycassaLogger()
        >>> log.set_logger_name('pycassa_library')
        >>> log.set_logger_level('debug')
        >>> log.get_logger().addHandler(logging.StreamHandler())

    """

    __shared_state = {}

    def __init__(self):
        self.__dict__ = self.__shared_state
        if not hasattr(self, '_has_been_initialized'):
            self._has_been_initialized = True
            self._root_logger = None
            self._logger_name = None
            self._level = None
            self._child_loggers = []
            self.set_logger_name(_DEFAULT_LOGGER_NAME)
            self.set_logger_level(_DEFAULT_LEVEL)

    def get_logger(self):
        """ Returns the underlying :class:`logging.Logger` instance. """
        return self._root_logger

    def set_logger_level(self, level):
        """ Sets the logging level for all pycassa logging. """
        self._level = level
        self._root_logger.setLevel(levels[level])

    def get_logger_level(self):
        """ Gets the logging level for all pycassa logging. """
        return self._level

    def set_logger_name(self, logger_name):
        """ Sets the root logger name for pycassa and all of its children loggers. """
        self._logger_name = logger_name
        self._root_logger = logging.getLogger(logger_name)
        h = NullHandler()
        self._root_logger.addHandler(h)
        for child_logger in self._child_loggers:
            # make the callback
            child_logger[2](logging.getLogger('%s.%s' % (logger_name, child_logger[1])))
        if self._level is not None:
            self.set_logger_level(self._level)

    def get_logger_name(self):
        """ Gets the root logger name for pycassa. """
        return self._logger_name

    def add_child_logger(self, child_logger_name, name_change_callback):
        """
        Adds a child logger to pycassa that will be
        updated when the logger name changes.

        """
        new_logger = logging.getLogger('%s.%s' % (self._logger_name, child_logger_name))
        self._child_loggers.append((new_logger, child_logger_name, name_change_callback))
        return new_logger

class NullHandler(logging.Handler):
    """ For python pre 2.7 compatibility. """
    def emit(self, record):
        pass

# Initialize our "singleton"
PycassaLogger()
