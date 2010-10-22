"""Logging facilities for pycassa."""

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
        return self._root_logger

    def set_logger_level(self, level):
        self._level = level
        self._root_logger.setLevel(levels[level]);

    def get_logger_level(self):
        return self._level

    def set_logger_name(self, logger_name):
        self._logger_name = logger_name
        self._root_logger = logging.getLogger(logger_name)
        h = NullHandler()
        self._root_logger.addHandler(h)
        for child_logger in self._child_loggers:
            # make the callback
            child_logger[2](logging.getLogger('%s.%s' % (logger_name, child_logger[1])))

    def get_logger_name(self):
        return self._logger_name

    def add_child_logger(self, child_logger_name, name_change_callback):
        new_logger = logging.getLogger('%s.%s' % (self._logger_name, child_logger_name))
        self._child_loggers.append((new_logger, child_logger_name, name_change_callback))
        return new_logger

class NullHandler(logging.Handler):
    def emit(self, record):
        pass

# Initialize our "singleton"
PycassaLogger()
