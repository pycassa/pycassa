from pycassa.cassandra.c08.ttypes import *

class KsDef(KsDef):

    def to07(self):
        if self.strategy_options and 'replication_factor' in self.strategy_options:
            if self.replication_factor is None:
                self.replication_factor = int(self.strategy_options['replication_factor'])
        return self

    def to08(self):
        if self.replication_factor is not None:
            if self.strategy_options is None:
                self.strategy_options = {}
            if 'replication_factor' not in self.strategy_options:
                self.strategy_options['replication_factor'] = str(self.replication_factor)
        return self
