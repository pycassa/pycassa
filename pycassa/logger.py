import logging

class PycassaLogger:

    __shared_state = {}

    _LOG_FILENAME = '/var/log/pycassa/system.log'

    _levels = {'debug': logging.DEBUG,
               'info': logging.INFO,
               'warn': logging.WARN,
               'error': logging.ERROR,
               'critical': logging.CRITICAL}

    def __init__(self, level='info', logger_name='pycassa'):
        self.__dict__ = self.__shared_state
        level = PycassaLogger._levels[level]
        logging.basicConfig(level=level)
        self.logger = logging.getLogger(logger_name)
        self.pool_logger = logging.getLogger('%s.pool' % logger_name)


    ### Connection Pool Logging ###

    def connect(self, dic):
        level = PycassaLogger._levels[dic.get('level', 'info')]
        if level <= logging.INFO:
            self.pool_logger.log(level,
                    "Connection opened for %s (id = %s)",
                    dic.get('pool_type'), dic.get('pool_id'))
        else:
            self.pool_logger.log(level,
                    "Error opening connection for %s (id = %s): %s",
                    dic.get('pool_type'), dic.get('pool_id'),
                    dic.get('error'))

    def checkout(self, dic):
        level = PycassaLogger._levels[dic.get('level', 'info')]
        self.pool_logger.log(level,
                "Connection was checked out from %s (id = %s)",
                dic.get('pool_type'), dic.get('pool_id'))

    def checkin(self, dic):
        level = PycassaLogger._levels[dic.get('level', 'info')]
        self.pool_logger.log(level,
                "Connection was checked in to %s (id = %s)",
                dic.get('pool_type'), dic.get('pool_id'))
 
    def close(self, dic):
        level = PycassaLogger._levels[dic.get('level', 'info')]
        if level <= logging.INFO:
            self.pool_logger.log(level,
                    "Connection was closed; pool %s (id = %s), reason: %s",
                    dic.get('pool_type'), dic.get('pool_id'), dic.get('message'))
        else:
            error = dic.get('error')
            self.pool_logger.log(level,
                    "Error closing connection in pool %s (id = %s), "
                    "reason: %s, error: %s %s",
                    dic.get('pool_type'), dic.get('pool_id'),
                    dic.get('message'), error.__class__, error)

    def server_list_obtained(self, dic):
        level = PycassaLogger._levels[dic.get('level', 'info')]
        self.pool_logger.log(level,
                "Server list obtained for %s (id = %s): %s",
                 dic.get('pool_type'), dic.get('pool_id'), ", ".join(dic.get('server_list')))

    def pool_recreated(self, dic):
        level = PycassaLogger._levels[dic.get('level', 'info')]
        self.pool_logger.log(level,
                "%s (id = %s) was recreated",
                dic.get('pool_type'), dic.get('pool_id'))

    def pool_disposed(self, dic):
        level = PycassaLogger._levels[dic.get('level', 'info')]
        self.pool_logger.log(level,
                "%s (id = %s) was disposed",
                dic.get('pool_type'), dic.get('pool_id'))

    def pool_max(self, dic):
        level = PycassaLogger._levels[dic.get('level', 'info')]
        self.pool_logger.log(level,
                "%s (id = %s) had a checkout request but was already "
                "at its max size (%s)",
                dic.get('pool_type'), dic.get('pool_id'), dic.get('pool_max'))
