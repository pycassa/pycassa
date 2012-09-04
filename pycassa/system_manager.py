import time

from pycassa.connection import Connection
from pycassa.cassandra.ttypes import IndexType, KsDef, CfDef, ColumnDef,\
                                     SchemaDisagreementException
import pycassa.marshal as marshal
import pycassa.types as types

_DEFAULT_TIMEOUT = 30
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

KEYS_INDEX = IndexType.KEYS
""" A secondary index type where each indexed value receives its own row """

BYTES_TYPE = types.BytesType()
LONG_TYPE = types.LongType()
INT_TYPE = types.IntegerType()
ASCII_TYPE = types.AsciiType()
UTF8_TYPE = types.UTF8Type()
TIME_UUID_TYPE = types.TimeUUIDType()
LEXICAL_UUID_TYPE = types.LexicalUUIDType()
COUNTER_COLUMN_TYPE = types.CounterColumnType()
DOUBLE_TYPE = types.DoubleType()
FLOAT_TYPE = types.FloatType()
DECIMAL_TYPE = types.DecimalType()
BOOLEAN_TYPE = types.BooleanType()
DATE_TYPE = types.DateType()

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
        >>> sys.create_keyspace('TestKeyspace', SIMPLE_STRATEGY, {'replication_factor': '1'})
        >>> sys.create_column_family('TestKeyspace', 'TestCF', super=False,
        ...                          comparator_type=LONG_TYPE)
        >>> sys.alter_column_family('TestKeyspace', 'TestCF', key_cache_size=42, gc_grace_seconds=1000)
        >>> sys.drop_keyspace('TestKeyspace')
        >>> sys.close()

    """

    def __init__(self, server='localhost:9160', credentials=None, framed_transport=True,
                 timeout=_DEFAULT_TIMEOUT):
        self._conn = Connection(None, server, framed_transport, timeout, credentials)

    def close(self):
        """ Closes the underlying connection """
        self._conn.close()

    def get_keyspace_column_families(self, keyspace, use_dict_for_col_metadata=False):
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

    def get_keyspace_properties(self, keyspace):
        """
        Gets a keyspace's properties.

        Returns a :class:`dict` with 'strategy_class' and
        'strategy_options' as keys.
        """
        if keyspace is None:
            keyspace = self._keyspace

        ks_def = self._conn.describe_keyspace(keyspace)
        return {'replication_strategy': ks_def.strategy_class,
                'strategy_options': ks_def.strategy_options}

    def list_keyspaces(self):
        """ Returns a list of all keyspace names. """
        return [ks.name for ks in self._conn.describe_keyspaces()]

    def describe_ring(self, keyspace):
        """ Describes the Cassandra cluster """
        return self._conn.describe_ring(keyspace)

    def describe_cluster_name(self):
        """ Gives the cluster name """
        return self._conn.describe_cluster_name()

    def describe_version(self):
        """ Gives the server's API version """
        return self._conn.describe_version()

    def describe_schema_versions(self):
        """ Lists what schema version each node has """
        return self._conn.describe_schema_versions()

    def describe_partitioner(self):
        """ Gives the partitioner that the cluster is using """
        part = self._conn.describe_partitioner()
        return part[part.rfind('.') + 1:]

    def describe_snitch(self):
        """ Gives the snitch that the cluster is using """
        snitch = self._conn.describe_snitch()
        return snitch[snitch.rfind('.') + 1:]

    def _system_add_keyspace(self, ksdef):
        return self._schema_update(self._conn.system_add_keyspace, ksdef)

    def _system_update_keyspace(self, ksdef):
        return self._schema_update(self._conn.system_update_keyspace, ksdef)

    def create_keyspace(self, name,
                        replication_strategy=SIMPLE_STRATEGY,
                        strategy_options=None, durable_writes=True, **ks_kwargs):

        """
        Creates a new keyspace.  Column families may be added to this keyspace
        after it is created using :meth:`create_column_family()`.

        `replication_strategy` determines how replicas are chosen for this keyspace.
        The strategies that Cassandra provides by default
        are available as :const:`SIMPLE_STRATEGY`, :const:`NETWORK_TOPOLOGY_STRATEGY`,
        and :const:`OLD_NETWORK_TOPOLOGY_STRATEGY`.

        `strategy_options` is a dictionary of strategy options. For
        NetworkTopologyStrategy, the dictionary should look like
        ``{'Datacenter1': '2', 'Datacenter2': '1'}``. This maps each
        datacenter (as defined in a Cassandra property file) to a replica count.
        For SimpleStrategy, you can specify the replication factor as follows:
        ``{'replication_factor': '1'}``.

        Example Usage:

        .. code-block:: python

            >>> from pycassa.system_manager import *
            >>> sys = SystemManager('192.168.10.2:9160')
            >>> # Create a SimpleStrategy keyspace
            >>> sys.create_keyspace('SimpleKS', SIMPLE_STRATEGY, {'replication_factor': '1'})
            >>> # Create a NetworkTopologyStrategy keyspace
            >>> sys.create_keyspace('NTS_KS', NETWORK_TOPOLOGY_STRATEGY, {'DC1': '2', 'DC2': '1'})
            >>> sys.close()

        """

        if replication_strategy.find('.') == -1:
            strategy_class = 'org.apache.cassandra.locator.%s' % replication_strategy
        else:
            strategy_class = replication_strategy

        ksdef = KsDef(name, strategy_class=strategy_class,
                      strategy_options=strategy_options,
                      cf_defs=[],
                      durable_writes=durable_writes)

        for k, v in ks_kwargs.iteritems():
            setattr(ksdef, k, v)

        self._system_add_keyspace(ksdef)

    def alter_keyspace(self, keyspace, replication_strategy=None,
                       strategy_options=None, durable_writes=None, **ks_kwargs):

        """
        Alters an existing keyspace.

        .. warning:: Don't use this unless you know what you are doing.

        Parameters are the same as for :meth:`create_keyspace()`.

        """

        old_ksdef = self._conn.describe_keyspace(keyspace)
        old_durable = getattr(old_ksdef, 'durable_writes', True)
        ksdef = KsDef(name=old_ksdef.name,
                      strategy_class=old_ksdef.strategy_class,
                      strategy_options=old_ksdef.strategy_options,
                      cf_defs=[],
                      durable_writes=old_durable)

        if replication_strategy is not None:
            if replication_strategy.find('.') == -1:
                ksdef.strategy_class = 'org.apache.cassandra.locator.%s' % replication_strategy
            else:
                ksdef.strategy_class = replication_strategy
        if strategy_options is not None:
            ksdef.strategy_options = strategy_options
        if durable_writes is not None:
            ksdef.durable_writes = durable_writes

        for k, v in ks_kwargs.iteritems():
            setattr(ksdef, k, v)

        self._system_update_keyspace(ksdef)

    def drop_keyspace(self, keyspace):
        """
        Drops a keyspace from the cluster.

        """
        self._schema_update(self._conn.system_drop_keyspace, keyspace)

    def _system_add_column_family(self, cfdef):
        self._conn.set_keyspace(cfdef.keyspace)
        return self._schema_update(self._conn.system_add_column_family, cfdef)

    def create_column_family(self, keyspace, name, column_validation_classes=None, **cf_kwargs):

        """
        Creates a new column family in a given keyspace.  If a value is not
        supplied for any of optional parameters, Cassandra will use a reasonable
        default value.

        `keyspace` should be the name of the keyspace the column family will
        be created in. `name` gives the name of the column family.
        """

        self._conn.set_keyspace(keyspace)
        cfdef = CfDef()
        cfdef.keyspace = keyspace
        cfdef.name = name

        if cf_kwargs.pop('super', False):
            cf_kwargs.setdefault('column_type', 'Super')

        for k, v in cf_kwargs.iteritems():
            v = self._convert_class_attrs(k, v)
            setattr(cfdef, k, v)

        if column_validation_classes:
            for (colname, value_type) in column_validation_classes.items():
                cfdef = self._alter_column_cfdef(cfdef, colname, value_type)

        self._system_add_column_family(cfdef)

    def _system_update_column_family(self, cfdef):
        return self._schema_update(self._conn.system_update_column_family, cfdef)

    def alter_column_family(self, keyspace, column_family, column_validation_classes=None, **cf_kwargs):
        """
        Alters an existing column family.

        Parameter meanings are the same as for :meth:`create_column_family`.
        """

        self._conn.set_keyspace(keyspace)
        cfdef = self.get_keyspace_column_families(keyspace)[column_family]

        for k, v in cf_kwargs.iteritems():
            v = self._convert_class_attrs(k, v)
            setattr(cfdef, k, v)

        if column_validation_classes:
            for (colname, value_type) in column_validation_classes.items():
                cfdef = self._alter_column_cfdef(cfdef, colname, value_type)

        self._system_update_column_family(cfdef)

    def drop_column_family(self, keyspace, column_family):
        """
        Drops a column family from the keyspace.

        """
        self._conn.set_keyspace(keyspace)
        self._schema_update(self._conn.system_drop_column_family, column_family)

    def _convert_class_attrs(self, attr, value):
        if attr in ('comparator_type', 'subcomparator_type',
                    'key_validation_class', 'default_validation_class'):
            return self._qualify_type_class(value)
        else:
            return value

    def _qualify_type_class(self, classname):
        if classname:
            if isinstance(classname, types.CassandraType):
                s = str(classname)
            elif isinstance(classname, basestring):
                s = classname
            else:
                raise TypeError(
                        "Column family validators and comparators " \
                        "must be specified as instances of " \
                        "pycassa.types.CassandraType subclasses or strings.")

            if s.find('.') == -1:
                return 'org.apache.cassandra.db.marshal.%s' % s
            else:
                return s
        else:
            return None

    def _alter_column_cfdef(self, cfdef, column, value_type):
        if cfdef.column_type == 'Super':
            packer = marshal.packer_for(cfdef.subcomparator_type)
        else:
            packer = marshal.packer_for(cfdef.comparator_type)

        packed_column = packer(column)

        value_type = self._qualify_type_class(value_type)
        cfdef.column_metadata = cfdef.column_metadata or []
        matched = False
        for c in cfdef.column_metadata:
            if c.name == packed_column:
                c.validation_class = value_type
                matched = True
                break
        if not matched:
            cfdef.column_metadata.append(ColumnDef(packed_column, value_type, None, None))

        return cfdef

    def alter_column(self, keyspace, column_family, column, value_type):
        """
        Sets a data type for the value of a specific column.

        `value_type` is a string that determines what type the column value will be.
        By default, :const:`LONG_TYPE`, :const:`INT_TYPE`,
        :const:`ASCII_TYPE`, :const:`UTF8_TYPE`, :const:`TIME_UUID_TYPE`,
        :const:`LEXICAL_UUID_TYPE` and :const:`BYTES_TYPE` are provided.  Custom
        types may be used as well by providing the class name; if the custom
        comparator class is not in ``org.apache.cassandra.db.marshal``, the fully
        qualified class name must be given.

        For super column families, this sets the subcolumn value type for
        any subcolumn named `column`, regardless of the super column name.

        """

        self._conn.set_keyspace(keyspace)
        cfdef = self.get_keyspace_column_families(keyspace)[column_family]
        self._system_update_column_family(self._alter_column_cfdef(cfdef, column, value_type))

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
            >>> sys.create_index('Keyspace1', 'Standard1', 'birthdate', LONG_TYPE, index_name='bday_index')
            >>> sys.close

        """

        self._conn.set_keyspace(keyspace)
        cfdef = self.get_keyspace_column_families(keyspace)[column_family]

        packer = marshal.packer_for(cfdef.comparator_type)
        packed_column = packer(column)

        value_type = self._qualify_type_class(value_type)
        coldef = ColumnDef(packed_column, value_type, index_type, index_name)

        for c in cfdef.column_metadata:
            if c.name == packed_column:
                cfdef.column_metadata.remove(c)
                break
        cfdef.column_metadata.append(coldef)
        self._system_update_column_family(cfdef)

    def drop_index(self, keyspace, column_family, column):
        """
        Drops an index on a column.

        """
        self._conn.set_keyspace(keyspace)
        cfdef = self.get_keyspace_column_families(keyspace)[column_family]

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

            # ignore unreachable nodes
            live_versions = [key for key in versions.keys() if key != 'UNREACHABLE']

            if len(live_versions) == 1:
                break
            else:
                time.sleep(_SAMPLE_PERIOD)

    def _schema_update(self, schema_func, *args):
        """
        Call schema updates functions and properly
        waits for agreement if needed.
        """
        while True:
            try:
                schema_version = schema_func(*args)
            except SchemaDisagreementException:
                self._wait_for_agreement()
            else:
                break
        return schema_version
