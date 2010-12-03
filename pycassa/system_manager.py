from logging.pycassa_logger import *
import time

from connection import Connection
from pycassa.cassandra.ttypes import IndexType, KsDef, CfDef, ColumnDef

_TIMEOUT = 5
_SAMPLE_PERIOD = 0.25

SIMPLE_STRATEGY = 'SimpleStrategy'
""" Replication strategy that simply chooses consecutive nodes in the ring for replicas """

NETWORK_TOPOLOGY_STRATEGY = 'NetworkTopologyStrategy'
""" Replication strategy that puts a number of replicas in each datacenter """

OLD_NETWORK_TOPOLOGY_STRATEGY = 'OldNetworkTopologyStrategy'
"""
Original replication strategy for putting a number of replicas in each datacenter.
This was originally called 'RackAwareStrategy'.
"""

BYTES_TYPE = 'BytesType'
""" Stores data as a byte array """
LONG_TYPE = 'LongType'
""" Stores data as an 8 byte integer """
INT_TYPE = 'IntegerType'
""" Stores data as an 4 byte integer """
ASCII_TYPE = 'AsciiType'
""" Stores data as ASCII text """
UTF8_TYPE = 'UTF8Type'
""" Stores data as UTF8 encoded text """
TIME_UUID_TYPE = 'TimeUUIDType'
""" Stores data as a version 1 UUID """
LEXICAL_UUID_TYPE = 'LexicalUUIDType'
""" Stores data as a non-version 1 UUID """

KEYS_INDEX = IndexType.KEYS
""" A secondary index type where each indexed value receives its own row """


class SystemManager(object):
    """
    Lets you examine and modify schema definitions as well as get basic
    information about the cluster.

    This class is mainly designed to be used manually in a python shell,
    not as part of a program, although it can be used that way.

    All operations which modify a keyspace or column family definition
    will block until the cluster reports that all nodes have accepted
    the modification.

    Example Usage:

    .. code-block:: python

        >>> from pycassa.system_manager import *
        >>> sys = SystemManager('192.168.10.2:9160')
        >>> sys.create_keyspace('TestKeyspace', replication_factor=1)
        >>> sys.create_column_family('TestKeyspace', 'TestCF', column_type='Standard',
        ...                          comparator_type=LONG_TYPE)
        >>> sys.alter_column_family('TestKeyspace', 'TestCF', key_cache_size=42, gc_grace_seconds=1000)
        >>> sys.drop_keyspace('TestKeyspace')
        >>> sys.close()

    """

    def __init__(self, server='localhost:9160', credentials=None, framed_transport=True):
        self._conn = Connection(None, server, framed_transport, _TIMEOUT, credentials)

    def close(self):
        """ Closes the underlying connection """
        self._conn.close()

    def get_keyspace_description(self, keyspace, use_dict_for_col_metadata=False):
        """
        Returns a raw description of the keyspace, which is more useful for use
        in programs than :meth:`describe_keyspace()`.
        
        If `use_dict_for_col_metadata` is ``True``, the CfDef's column_metadata will
        be stored as a dictionary where the keys are column names instead of a list.

        Returns a dictionary of the form ``{column_family_name: CfDef}``

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
        """
        Returns a human readable description of the Keyspace.

        For use in a program, use :meth:`get_keyspace_description()` instead.

        """
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
        """ Returns a human readable description of the Column Family """

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
        """ Describes the Cassandra cluster """
        return self._conn.describe_ring(keyspace)

    def describe_cluster_name(self):
        """ Gives the cluster name """
        return self._conn.describe_cluster_name()

    def describe_version(self):
        """ Gives the server's API version """
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

        """
        Creates a new keyspace.  Column families may be added to this keyspace
        after it is created using :meth:`create_column_family()`.

        `replication_strategy` determines how replicas are chosen for this keyspace.
        The strategies that Cassandra provides by default
        are available as :const:`SIMPLE_STRATEGY`, :const:`NETWORK_TOPOLOGY_STRATEGY`,
        and :const:`OLD_NETWORK_TOPOLOGY_STRATEGY`.  `NETWORK_TOPOLOGY_STRATEGY` requires
        `strategy_options` to be present.

        `strategy_options` is an optional dictionary of strategy options. By default, these
        are only used by NetworkTopologyStrategy; in this case, the dictionary should
        look like: ``{'Datacenter1': '2', 'Datacenter2': '1'}``.  This maps each
        datacenter (as defined in a Cassandra property file) to a replica count.

        Example Usage:

        .. code-block:: python

            >>> from pycassa.system_manager import *
            >>> sys = SystemManager('192.168.10.2:9160')
            >>> # Create a SimpleStrategy keyspace
            >>> sys.create_keyspace('SimpleKS', 1)
            >>> # Create a NetworkTopologyStrategy keyspace
            >>> sys.create_keyspace('NTS_KS', 3, NETWORK_TOPOLOGY_STRATEGY, {'DC1': '2', 'DC2': '1'})
            >>> sys.close()

        """

        if replication_strategy.find('.') == -1:
            strategy_class = 'org.apache.cassandra.locator.%s' % replication_strategy
        else:
            strategy_class = replication_strategy
        ksdef = KsDef(name, strategy_class, strategy_options, replication_factor, [])
        self._system_add_keyspace(ksdef)

    def alter_keyspace(self, keyspace, replication_factor=None,
                       replication_strategy=None,
                       strategy_options=None):

        """
        Alters an existing keyspace. 

        .. warning:: Don't use this unless you know what you are doing.

        Parameters are the same as for :meth:`create_keyspace()`.

        """

        ksdef = self._conn.describe_keyspace(keyspace)

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
        Drops a keyspace from the cluster.

        """
        schema_version = self._conn.system_drop_keyspace(keyspace)
        self._wait_for_agreement()
        return schema_version

    def _system_add_column_family(self, cfdef):
        self._conn.set_keyspace(cfdef.keyspace)
        schema_version = self._conn.system_add_column_family(cfdef) 
        self._wait_for_agreement()
        return schema_version

    def create_column_family(self, keyspace, name, super=False,
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
                             memtable_operations_in_millions=None,
                             comment=None):

        """
        Creates a new column family in a given keyspace.  If a value is not
        supplied for any of optional parameters, Cassandra will use a reasonable
        default value.

        :param str keyspace: what keyspace the column family will be created in

        :param str name: the name of the column family

        :param bool super: Whether or not this column family is a super column family

        :param str comparator_type: What type the column names will be, which affects
          their sort order.  By default, :const:`LONG_TYPE`, :const:`INTEGER_TYPE`,
          :const:`ASCII_TYPE`, :const:`UTF8_TYPE`, :const:`TIME_UUID_TYPE`,
          :const:`LEXICAL_UUID_TYPE` and :const:`BYTES_TYPE` are provided.  Custom
          types may be used as well by providing the class name; if the custom
          comparator class is not in ``org.apache.cassandra.db.marshal``, the fully
          qualified class name must be given.

        :param str subcomparator_type: Like `comparator_type`, but if the column family
          is a super column family, this applies to the type of the subcolumn names

        :param int or float key_cache_size: The size of the key cache, either in a
          percentage of total keys (0.15, for example) or in an absolute number of
          keys (20000, for example).

        :param int or float row_cache_size: Same as `key_cache_size`, but for the row cache

        :param int gc_grace_seconds: Number of seconds before tombstones are removed

        :param float read_repair_chance: probability of a read repair occuring

        :param str default_validation_class: the data type for all column values in the CF.
          the choices for this are the same as for `comparator_type`.

        :param int min_compaction_threshold: Number of similarly sized SSTables that must
          be present before a minor compaction is scheduled. Setting to 0 disables minor
          compactions.

        :param int max_compaction_threshold: Number of similarly sized SSTables that must
          be present before a minor compaction is performed immediately. Setting to 0
          disables minor compactions.

        :param int key_cache_save_in_seconds: How often the key cache should be saved; this
          helps to avoid a cold cache on restart

        :param int row_cache_save_in_seconds: How often the row cache should be saved; this
          helps to avoid a cold cache on restart

        :param int memtable_flush_after_mins: Memtables are flushed when they reach this age

        :param int memtable_throughput_in_mb: Memtables are flushed when this many MBs have
          been written to them

        :param int memtable_operations_in_millions: Memtables are flushed when this many million
          operations have been performed on them

        :param str comment: A human readable description

        """

        self._conn.set_keyspace(keyspace)
        cfdef = CfDef()
        cfdef.keyspace = keyspace
        cfdef.name = name

        if super:
            cfdef.column_type = 'Super'

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

        if memtable_operations_in_millions is not None:
            if memtable_operations_in_millions < 0:
                ire = InvalidRequestException()
                ire.why = 'memtable_operations_in_millions must be non-negative'
                raise ire
            cfdef.memtable_operations_in_millions = memtable_operations_in_millions

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
                            memtable_operations_in_millions=None,
                            comment=None):

        """
        Alters an existing column family.

        Parameter meanings are the same as for :meth:`create_column_family`,
        but column family attributes which may not be modified are not
        included here.

        """

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

        if memtable_operations_in_millions is not None:
            if memtable_operations_in_millions < 0:
                ire = InvalidRequestException()
                ire.why = 'memtable_operations_in_millions must be non-negative'
                raise ire
            cfdef.memtable_operations_in_millions = memtable_operations_in_millions

        if comment is not None:
            cfdef.comment = comment

        self._system_update_column_family(cfdef)

    def drop_column_family(self, keyspace, column_family):
        """
        Drops a column family from the keyspace.

        """
        self._conn.set_keyspace(keyspace)
        schema_version = self._conn.system_drop_column_family(column_family)
        self._wait_for_agreement()
        return schema_version

    def alter_column(self, keyspace, column_family, column, value_type):
        """
        Sets a data type for the value of a specific column.

        `value_type` is a string that determines what type the column value will be.
        By default, :const:`LONG_TYPE`, :const:`INTEGER_TYPE`,
        :const:`ASCII_TYPE`, :const:`UTF8_TYPE`, :const:`TIME_UUID_TYPE`,
        :const:`LEXICAL_UUID_TYPE` and :const:`BYTES_TYPE` are provided.  Custom
        types may be used as well by providing the class name; if the custom
        comparator class is not in ``org.apache.cassandra.db.marshal``, the fully
        qualified class name must be given.

        """

        self._conn.set_keyspace(keyspace)
        cfdef = self.get_keyspace_description(keyspace)[column_family]

        if value_type.find('.') == -1:
            value_type = 'org.apache.cassandra.db.marshal.%s' % value_type

        matched = False
        for c in cfdef.column_metadata:
            if c.name == column:
                c.validation_class = value_type
                matched = True
                break
        if not matched:
            cfdef.column_metadata.append(ColumnDef(column, value_type, None, None))
        self._system_update_column_family(cfdef)

    def create_index(self, keyspace, column_family, column, value_type,
                     index_type=KEYS_INDEX, index_name=None):
        """
        Creates an index on a column.

        This allows efficient for index usage via 
        :meth:`~pycassa.columnfamily.ColumnFamily.get_indexed_slices()`

        `column` specifies what column to index, and `value_type` is a string
        that describes that column's value's data type; see
        :meth:`alter_column()` for a full description of `value_type`.

        `index_type` determines how the index will be stored internally. Currently,
        :const:`KEYS_INDEX` is the only option.  `index_name` is an optional name
        for the index.

        Example Usage:

        .. code-block:: python

            >>> from pycassa.system_manager import *
            >>> sys = SystemManager('192.168.2.10:9160')
            >>> sys.create_index('Keyspace1', 'Standard1', 'birthdate', LONG_TYPE, 'bday_index')
            >>> sys.close

        """

        self._conn.set_keyspace(keyspace)
        cfdef = self.get_keyspace_description(keyspace)[column_family]

        if value_type.find('.') == -1:
            value_type = 'org.apache.cassandra.db.marshal.%s' % value_type

        coldef = ColumnDef(column, value_type, index_type, index_name)

        matched = False
        for c in cfdef.column_metadata:
            if c.name == column:
                c = coldef
                matched = True
                break
        if not matched:
            cfdef.column_metadata.append(coldef)
        self._system_update_column_family(cfdef)

    def drop_index(self, keyspace, column_family, column):
        """
        Drops an index on a column.

        """
        self._conn.set_keyspace(keyspace)
        cfdef = self.get_keyspace_description(keyspace)[column_family]

        matched = False
        for c in cfdef.column_metadata:
            if c.name == column:
                c.index_type = None
                c.index_name = None
                matched = True
                break

        if matched:
            self._system_update_column_family(cfdef)

    def _wait_for_agreement(self): 
        while True:
            versions = self._conn.describe_schema_versions()
            if len(versions) == 1:
                break
            time.sleep(_SAMPLE_PERIOD)
