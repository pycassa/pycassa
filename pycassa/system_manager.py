from logging.pycassa_logger import *
import time

from pycassa.cassandra import Cassandra
from pycassa.cassandra.ttypes import KsDef

__all__ = ['SystemManager']

class SystemManager(object):

    def __init__(server='localhost:9160', credentials=None, framed_transport=True):
        self.conn = ClientTransport(None, server, framed_transport, 0.5, credentials)

    def close(self):
        self.conn.close()

    def describe_keyspace(self, keyspace, use_dict_for_col_metadata=False):
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

        ks_def = self.describe_keyspace(keyspace)
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

    def describe_ring(self, keyspace):
        return self.conn.client.describe_ring(keyspace)

    def describe_cluster_name(self):
        return self.conn.client.describe_cluster_name()

    def describe_version(self):
        return self.conn.client.describe_version()

    def add_keyspace(self, ksdef, block=True, sample_period=0.25):
        """
        Adds a keyspace to the cluster.

        :param ksdef: the keyspace definition
        :type ksdef: :class:`~pycassa.cassandra.ttypes.KsDef`

        :param block: wait until the cluster has the same schema version
          to return

        :param sample_period: when blocking, how often to check for schema
          agreement among the cluster (in seconds)
        :type sample_period: int

        :rtype: new schema version

        """
        schema_version = self.conn.client.system_add_keyspace(ksdef) 
        if block:
            self._wait_for_agreement(sample_period)
        return schema_version

    def update_keyspace(self, ksdef, block=True, sample_period=0.25):
        """
        Updates a keyspace.

        :param ksdef: the new keyspace definition
        :type ksdef: :class:`~pycassa.cassandra.ttypes.KsDef`

        :param block: wait until the cluster has the same schema version
          to return

        :param sample_period: when blocking, how often to check for schema
          agreement among the cluster (in seconds)
        :type sample_period: int

        :rtype: new schema version

        """
        schema_version = self.conn.client.system_update_keyspace(ksdef) 
        if block:
            self._wait_for_agreement(sample_period)
        return schema_version

    def drop_keyspace(self, name, block=True, sample_period=0.25):
        """
        Drop a keyspace from the cluster.

        :param ksdef: the keyspace to drop
        :type ksdef: string

        :param block: wait until the cluster has the same schema version
          to return

        :param sample_period: when blocking, how often to check for schema
          agreement among the cluster (in seconds)
        :type sample_period: int

        :rtype: new schema version

        """
        schema_version = self.conn.client.system_drop_keyspace(name)
        if block:
            self._wait_for_agreement(sample_period)
        return schema_version

    def add_column_family(self, cfdef, block=True, sample_period=0.25):
        """
        Adds a column family to a keyspace.

        :param cfdef: the column family definition
        :type cfdef: :class:`~pycassa.cassandra.ttypes.CfDef`

        :param block: wait until the cluster has the same schema version
          to return

        :param sample_period: when blocking, how often to check for schema
          agreement among the cluster (in seconds)
        :type sample_period: int

        :rtype: new schema version

        """
        schema_version = self.conn.client.system_add_column_family(cfdef) 
        if block:
            self._wait_for_agreement(sample_period)
        return schema_version

    def update_column_family(self, cfdef, block=True, sample_period=0.25):
        """
        Update a column family definition.

        .. warning:: If you do not set values for *everything* in the
                     :class:`.CfDef`, Cassandra will fill them in with
                     *bad* values.  You should get the existing
                     :class:`.CfDef` using :meth:`get_keyspace_description()`,
                     change what you want, and use that.

        Example usage::

            >>> conn = pycassa.connect('Keyspace1')
            >>> cfdef = conn.get_keyspace_description()['MyColumnFamily']
            >>> cfdef.memtable_throughput_in_mb = 256
            >>> conn.update_column_family(cfdef)

        :param cfdef: the column family definition
        :type cfdef: :class:`~pycassa.cassandra.ttypes.CfDef`

        :param block: wait until the cluster has the same schema version
          to return

        :param sample_period: when blocking, how often to check for schema
          agreement among the cluster (in seconds)
        :type sample_period: int

        :rtype: new schema version

        """
        schema_version = self.conn.client.system_update_column_family(cfdef) 
        if block:
            self._wait_for_agreement(sample_period)
        return schema_version

    def drop_column_family(self, name, block=True, sample_period=0.25):
        """
        Drops a column family from the cluster.

        :param name: the column family to drop
        :type name: string

        :param block: wait until the cluster has the same schema version
          to return

        :param sample_period: when blocking, how often to check for schema
          agreement among the cluster (in seconds)
        :type sample_period: int

        :rtype: new schema version

        """
        schema_version = self.conn.client.system_drop_column_family(name)
        if block:
            self._wait_for_agreement(sample_period)
        return schema_version

    def _wait_for_agreement(self, sample_period): 
        while True:
            versions = self.conn.client.describe_schema_versions()
            if len(versions) == 1:
                break
            time.sleep(sample_period)
