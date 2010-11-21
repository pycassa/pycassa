from logging.pycassa_logger import *
import time

from connection import Connection
from pycassa.cassandra.ttypes import IndexType, KsDef, CfDef

__all__ = ['SystemManager']

_TIMEOUT = 5
_SAMPLE_PERIOD = 0.25

class SystemManager(object):

    SIMPLE_STRATEGY = 'SimpleStrategy'
    NETWORK_TOPOLOGY_STRATEGY = 'NetworkTopologyStrategy'
    OLD_NETWORK_TOPOLOGY_STRATEGY = 'OldNetworkTopologyStrategy'

    BYTES_TYPE = 'BytesType'
    LONG_TYPE = 'LongType'
    INT_TYPE = 'IntegerType'
    ASCII_TYPE = 'AsciiType'
    UTF8_TYPE = 'UTF8Type'
    TIME_UUID_TYPE = 'TimeUUIDType'
    LEXICAL_UUID_TYPE = 'LexicalUUIDType'

    def __init__(self, server='localhost:9160', credentials=None, framed_transport=True):
        self._conn = Connection(None, server, framed_transport, _TIMEOUT, credentials)

    def close(self):
        self._conn.close()

    def get_keyspace_description(self, keyspace, use_dict_for_col_metadata=False):
        """
        Describes the given keyspace.
        
        :param keyspace: The keyspace to describe. Defaults to the current keyspace.
        :type keyspace: str

        :param use_dict_for_col_metadata: whether or not store the column metadata as a
          dictionary instead of a list
        :type use_dict_for_col_metadata: bool

        :rtype: ``{column_family_name: CfDef}``

        """
        if keyspace is None:
            keyspace = self._keyspace

        ks_def = self._conn.describe_keyspace(keyspace)
        cf_defs = dict()
        for cf_def in ks_def.cf_defs:
            cf_defs[cf_def.name] = cf_def
            if use_dict_for_col_metadata:
                old_metadata = cf_def.column_metadata
                new_metadata = dict()
                for datum in old_metadata:
                    new_metadata[datum.name] = datum
                cf_def.column_metadata = new_metadata
        return cf_defs

    def describe_keyspace(self, keyspace):
        ksdef = self._conn.describe_keyspace(keyspace)

        print
        spaces = " " * (35 - len('Name:'))
        print "Name:", spaces, ksdef.name
        print

        spaces = " " * (35 - len('Replication Strategy:'))
        s = ksdef.strategy_class
        print "Replication Strategy:", spaces, s[s.rfind('.') + 1: ]

        if ksdef.strategy_options:
            spaces = " " * (35 - len('Strategy Options:'))
            print "Strategy Options:", spaces, ksdef.strategy_options

        spaces = " " * (35 - len('Replication Factor:'))
        print "Replication Factor:", spaces, ksdef.replication_factor
        print

        print "Column Families:"
        for cfdef in ksdef.cf_defs:
            print "  ", cfdef.name
        print

    def describe_column_family(self, keyspace, column_family):
        try:
            cfdef = self.get_keyspace_description(keyspace)[column_family]
        except KeyError:
            print "Column family %s does not exist in keyspace %s" % (column_family, keyspace)
            return

        print

        spaces = " " * (35 - len('Name:'))
        print "Name:", spaces, cfdef.name

        spaces = " " * (35 - len('Description:'))
        print "Description:", spaces, cfdef.comment

        spaces = " " * (35 - len('Column Type:'))
        print "Column Type:", spaces, cfdef.column_type
        print

        spaces = " " * (35 - len('Comparator Type:'))
        s = cfdef.comparator_type
        print "Comparator Type:", spaces, s[s.rfind('.') + 1: ]

        if cfdef.column_type == 'Super':
            spaces = " " * (35 - len('Subcomparator Type:'))
            s = cfdef.subcomparator_type
            print "Subcomparator Type:", spaces, s[s.rfind('.') + 1: ]

        spaces = " " * (35 - len('Default Validation Class:'))
        s = cfdef.default_validation_class
        print "Default Validation Class:", spaces, s[s.rfind('.') + 1: ]
        print

        print "Cache Sizes"
        spaces = " " * (35 - len('  Row Cache:'))
        if cfdef.row_cache_size == 0:
            s = 'Disabled'
        elif cfdef.row_cache_size >= 1:
            s = str(int(cfdef.row_cache_size)) + " rows"
        else:
            s = str(cfdef.key_cache_size) + "%"
        print "  Row Cache:", spaces, s

        spaces = " " * (35 - len('  Key Cache:'))
        if cfdef.key_cache_size == 0:
            s = 'Disabled'
        elif cfdef.key_cache_size >= 1:
            s = str(int(cfdef.key_cache_size)) + " keys"
        else:
            s = str(cfdef.key_cache_size) + "%"
        print "  Key Cache:", spaces, s
        print

        spaces = " " * (35 - len('Read Repair Chance:'))
        if cfdef.read_repair_chance == 0:
            s = 'Disabled'
        else:
            s = str(cfdef.read_repair_chance * 100) + '%'
        print "Read Repair Chance:", spaces, s
        print

        spaces = " " * (35 - len('GC Grace Seconds:'))
        print "GC Grace Seconds:", spaces, cfdef.gc_grace_seconds
        print

        compact_disabled = cfdef.min_compaction_threshold == 0 or cfdef.max_compaction_threshold == 0
            
        print "Compaction Thresholds"
        spaces = " " * (35 - len('  Min:'))
        if compact_disabled:
            print "  Min:", spaces, "Minor Compactions Disabled"
        else:
            print "  Min:", spaces, cfdef.min_compaction_threshold

        spaces = " " * (35 - len('  Max:'))
        if compact_disabled:
            print "  Max:", spaces, "Minor Compactions Disabled"
        else:
            print "  Max:", spaces, cfdef.max_compaction_threshold
        print

        print 'Memtable Flush After Thresholds'
        spaces = " " * (35 - len('  Throughput:'))
        print "  Throughput:", spaces, str(cfdef.memtable_throughput_in_mb), "MiB"
        spaces = " " * (35 - len('  Operations:'))
        s = str(int(cfdef.memtable_operations_in_millions * 1000000))
        print "  Operations:", spaces, s, "operations"
        spaces = " " * (35 - len('  Time:'))
        print "  Time:", spaces, str(cfdef.memtable_flush_after_mins), "minutes"
        print

        print "Cache Save Periods"
        spaces = " " * (35 - len('  Row Cache:'))
        if cfdef.row_cache_save_period_in_seconds == 0:
            s = 'Disabled'
        else:
            s = str(cfdef.row_cache_save_period_in_seconds) + ' seconds'
        print "  Row Cache:", spaces, s

        spaces = " " * (35 - len('  Key Cache:'))
        if cfdef.key_cache_save_period_in_seconds == 0:
            s = 'Disabled'
        else:
            s = str(cfdef.key_cache_save_period_in_seconds) + ' seconds'
        print "  Key Cache:", spaces, s
        print

        if cfdef.column_metadata:
            print "Column Metadata"
            for coldef in cfdef.column_metadata:
                spaces = " " * (35 - len('  - Name:'))
                print "  - Name:", spaces, coldef.name

                spaces = " " * (35 - len('    Value Type:'))
                s = coldef.validation_class
                print "    Value Type:", spaces, s[s.rfind('.') + 1: ]

                if coldef.index_type is not None:
                    spaces = " " * (35 - len('    Index Type:'))
                    s = IndexType._VALUES_TO_NAMES[coldef.index_type]
                    print "    Index Type:", spaces, s[s.rfind('.') + 1: ]

                    spaces = " " * (35 - len('    Index Name:'))
                    print "    Index Name:", spaces, coldef.index_name
                print

    def describe_ring(self, keyspace):
        return self._conn.describe_ring(keyspace)

    def describe_cluster_name(self):
        return self._conn.describe_cluster_name()

    def describe_version(self):
        return self._conn.describe_version()

    def _system_add_keyspace(self, ksdef):
        schema_version = self._conn.system_add_keyspace(ksdef) 
        self._wait_for_agreement()
        return schema_version

    def _system_update_keyspace(self, ksdef):
        schema_version = self._conn.system_update_keyspace(ksdef) 
        self._wait_for_agreement()
        return schema_version

    def create_keyspace(self, name, replication_factor,
                        replication_strategy=SIMPLE_STRATEGY,
                        strategy_options=None):

        if replication_strategy.find('.') == -1:
            strategy_class = 'org.apache.cassandra.locator.%s' % replication_strategy
        else:
            strategy_class = replication_strategy
        ksdef = KsDef(name, strategy_class, strategy_options, replication_factor, [])
        self._system_add_keyspace(ksdef)

    def alter_keyspace(self, keyspace,
                       replication_strategy=None,
                       strategy_options=None,
                       replication_factor=None):

        ksdef = self.describe_keyspace(keyspace)

        ksdef.cf_defs = []
        if replication_strategy is not None:
            if replication_strategy.find('.') == -1:
                ksdef.strategy_class = 'org.apache.cassandra.locator.%s' % replication_strategy
            else:
                ksdef.strategy_class = replication_strategy
        if strategy_options is not None:
            ksdef.strategy_options = strategy_options
        if replication_factor is not None:
            ksdef.replication_factor = replication_factor

        self._system_update_keyspace(ksdef)

    def drop_keyspace(self, keyspace):
        """
        Drop a keyspace from the cluster.

        :param keyspace: the keyspace to drop
        :type keyspace: string

        :rtype: new schema version

        """
        schema_version = self._conn.system_drop_keyspace(name)
        self._wait_for_agreement()
        return schema_version

    def _system_add_column_family(self, cfdef):
        self._conn.set_keyspace(cfdef.keyspace)
        schema_version = self._conn.system_add_column_family(cfdef) 
        self._wait_for_agreement()
        return schema_version

    def create_column_family(self, keyspace, name, column_type,
                             comparator_type=None,
                             subcomparator_type=None,
                             key_cache_size=None,
                             row_cache_size=None,
                             gc_grace_seconds=None,
                             read_repair_chance=None,
                             default_validation_class=None,
                             min_compaction_threshold=None,
                             max_compaction_threshold=None,
                             key_cache_save_period_in_seconds=None,
                             row_cache_save_period_in_seconds=None,
                             memtable_flush_after_mins=None,
                             memtable_throughput_in_mb=None,
                             memtable_throughput_in_millions=None,
                             comment=None):

        self._conn.set_keyspace(keyspace)
        cfdef = CfDef()
        cfdef.keyspace = keyspace
        cfdef.name = name

        if comparator_type is not None:
            if comparator_type.find('.') == -1:
                cfdef.comparator_type = 'org.apache.cassandra.db.marshal.%s' % comparator_type
            else:
                cfdef.comparator_type = comparator_type

        if subcomparator_type is not None:
            if subcomparator_type.find('.') == -1:
                cfdef.subcomparator_type = 'org.apache.cassandra.db.marshal.%s' % subcomparator_type
            else:
                cfdef.subcomparator_type = subcomparator_type

        if key_cache_size is not None:
            if key_cache_size < 0:
                ire = InvalidRequestException()
                ire.why = 'key_cache_size must be non-negative'
                raise ire
            cfdef.key_cache_size = key_cache_size

        if row_cache_size is not None:
            if row_cache_size < 0:
                ire = InvalidRequestException()
                ire.why = 'row_cache_size must be non-negative'
                raise ire
            cfdef.row_cache_size = row_cache_size

        if gc_grace_seconds is not None:
            if gc_grace_seconds < 0:
                ire = InvalidRequestException()
                ire.why = 'gc_grace_seconds must be non-negative'
                raise ire
            cfdef.gc_grace_seconds = gc_grace_seconds

        if read_repair_chance is not None:
            if read_repair_chance < 0:
                ire = InvalidRequestException()
                ire.why = 'read_repair_chance must be non-negative'
                raise ire
            cfdef.read_repair_chance = read_repair_chance

        if default_validation_class is not None:
            if default_validation_class.find('.') == -1:
                cfdef.default_validation_class = 'org.apache.cassandra.db.marshal.%s' % default_validation_class
            else:
                cfdef.default_validation_class = default_validation_class

        if min_compaction_threshold is not None:
            if min_compaction_threshold < 0:
                ire = InvalidRequestException()
                ire.why = 'min_compaction_threshold must be non-negative'
                raise ire
            cfdef.min_compaction_threshold = min_compaction_threshold

        if max_compaction_threshold is not None:
            if max_compaction_threshold < 0:
                ire = InvalidRequestException()
                ire.why = 'max_compaction_threshold must be non-negative'
                raise ire
            cfdef.max_compaction_threshold = max_compaction_threshold

        if key_cache_save_period_in_seconds is not None:
            if key_cache_save_period_in_seconds < 0:
                ire = InvalidRequestException()
                ire.why = 'key_cache_save_period_in_seconds must be non-negative'
                raise ire
            cfdef.key_cache_save_period_in_seconds = key_cache_save_period_in_seconds

        if row_cache_save_period_in_seconds is not None:
            if row_cache_save_period_in_seconds < 0:
                ire = InvalidRequestException()
                ire.why = 'row_cache_save_period_in_seconds must be non-negative'
                raise ire
            cfdef.row_cache_save_period_in_seconds = row_cache_save_period_in_seconds

        if memtable_flush_after_mins is not None:
            if memtable_flush_after_mins < 0:
                ire = InvalidRequestException()
                ire.why = 'memtable_flush_after_mins must be non-negative'
                raise ire
            cfdef.memtable_flush_after_mins = memtable_flush_after_mins

        if memtable_throughput_in_mb is not None:
            if memtable_throughput_in_mb < 0:
                ire = InvalidRequestException()
                ire.why = 'memtable_throughput_in_mb must be non-negative'
                raise ire
            cfdef.memtable_throughput_in_mb = memtable_throughput_in_mb

        if memtable_throughput_in_millions is not None:
            if memtable_throughput_in_millions < 0:
                ire = InvalidRequestException()
                ire.why = 'memtable_throughput_in_millions must be non-negative'
                raise ire
            cfdef.memtable_throughput_in_millions = memtable_throughput_in_millions

        if comment is not None:
            cfdef.comment = comment

        self._system_add_column_family(cfdef)

    def _system_update_column_family(self, cfdef):
        schema_version = self._conn.system_update_column_family(cfdef) 
        self._wait_for_agreement()
        return schema_version

    def alter_column_family(self, keyspace, column_family,
                            key_cache_size=None,
                            row_cache_size=None,
                            gc_grace_seconds=None,
                            read_repair_chance=None,
                            default_validation_class=None,
                            min_compaction_threshold=None,
                            max_compaction_threshold=None,
                            key_cache_save_period_in_seconds=None,
                            row_cache_save_period_in_seconds=None,
                            memtable_flush_after_mins=None,
                            memtable_throughput_in_mb=None,
                            memtable_throughput_in_millions=None,
                            comment=None):

        self._conn.set_keyspace(keyspace)
        cfdef = self.get_keyspace_description(keyspace)[column_family]

        if key_cache_size is not None:
            if key_cache_size < 0:
                ire = InvalidRequestException()
                ire.why = 'key_cache_size must be non-negative'
                raise ire
            cfdef.key_cache_size = key_cache_size

        if row_cache_size is not None:
            if row_cache_size < 0:
                ire = InvalidRequestException()
                ire.why = 'row_cache_size must be non-negative'
                raise ire
            cfdef.row_cache_size = row_cache_size

        if gc_grace_seconds is not None:
            if gc_grace_seconds < 0:
                ire = InvalidRequestException()
                ire.why = 'gc_grace_seconds must be non-negative'
                raise ire
            cfdef.gc_grace_seconds = gc_grace_seconds

        if read_repair_chance is not None:
            if read_repair_chance < 0:
                ire = InvalidRequestException()
                ire.why = 'read_repair_chance must be non-negative'
                raise ire
            cfdef.read_repair_chance = read_repair_chance

        if default_validation_class is not None:
            if default_validation_class.find('.') == -1:
                cfdef.default_validation_class = 'org.apache.cassandra.db.marshal.%s' % default_validation_class
            else:
                cfdef.default_validation_class = default_validation_class

        if min_compaction_threshold is not None:
            if min_compaction_threshold < 0:
                ire = InvalidRequestException()
                ire.why = 'min_compaction_threshold must be non-negative'
                raise ire
            cfdef.min_compaction_threshold = min_compaction_threshold

        if max_compaction_threshold is not None:
            if max_compaction_threshold < 0:
                ire = InvalidRequestException()
                ire.why = 'max_compaction_threshold must be non-negative'
                raise ire
            cfdef.max_compaction_threshold = max_compaction_threshold

        if key_cache_save_period_in_seconds is not None:
            if key_cache_save_period_in_seconds < 0:
                ire = InvalidRequestException()
                ire.why = 'key_cache_save_period_in_seconds must be non-negative'
                raise ire
            cfdef.key_cache_save_period_in_seconds = key_cache_save_period_in_seconds

        if row_cache_save_period_in_seconds is not None:
            if row_cache_save_period_in_seconds < 0:
                ire = InvalidRequestException()
                ire.why = 'row_cache_save_period_in_seconds must be non-negative'
                raise ire
            cfdef.row_cache_save_period_in_seconds = row_cache_save_period_in_seconds

        if memtable_flush_after_mins is not None:
            if memtable_flush_after_mins < 0:
                ire = InvalidRequestException()
                ire.why = 'memtable_flush_after_mins must be non-negative'
                raise ire
            cfdef.memtable_flush_after_mins = memtable_flush_after_mins

        if memtable_throughput_in_mb is not None:
            if memtable_throughput_in_mb < 0:
                ire = InvalidRequestException()
                ire.why = 'memtable_throughput_in_mb must be non-negative'
                raise ire
            cfdef.memtable_throughput_in_mb = memtable_throughput_in_mb

        if memtable_throughput_in_millions is not None:
            if memtable_throughput_in_millions < 0:
                ire = InvalidRequestException()
                ire.why = 'memtable_throughput_in_millions must be non-negative'
                raise ire
            cfdef.memtable_throughput_in_millions = memtable_throughput_in_millions

        if comment is not None:
            cfdef.comment = comment

        self._system_update_column_family(cfdef)

    def drop_column_family(self, keyspace, column_family):
        """
        Drops a column family from the cluster.

        :param name: the column family to drop
        :type name: string

        :rtype: new schema version

        """
        self._conn.set_keyspace(keyspace)
        schema_version = self._conn.system_drop_column_family(column_family)
        self._wait_for_agreement()
        return schema_version

    def _wait_for_agreement(self): 
        while True:
            versions = self._conn.describe_schema_versions()
            if len(versions) == 1:
                break
            time.sleep(_SAMPLE_PERIOD)
