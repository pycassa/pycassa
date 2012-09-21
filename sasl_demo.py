from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
from pycassa.connection import make_sasl_transport_factory
from pycassa.system_manager import SystemManager

transport_factory = make_sasl_transport_factory(
        sasl_host='thobbs-laptop',
        sasl_service='host',
        mechanism='GSSAPI'
)
sysman = SystemManager(transport_factory=transport_factory)
sysman.create_keyspace('Keyspace1', 'SimpleStrategy', {'replication_factor': '1'})
sysman.create_column_family('Keyspace1', 'CF1')
sysman.close()

pool = ConnectionPool('Keyspace1', transport_factory=transport_factory)
cf = ColumnFamily(pool, 'CF1')

for i in range(100):
    cf.insert('key%d' % i, {'col': 'val'})

for i in range(100):
    print 'key%d:' % i, cf.get('key%d' % i)

pool.dispose()
