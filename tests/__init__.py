from pycassa.system_manager import *
from pycassa.cassandra.constants import *

TEST_KS = 'PycassaTestKeyspace'

def setup_package():
    sys = SystemManager()
    if TEST_KS in sys.list_keyspaces():
        sys.drop_keyspace(TEST_KS)
    try:
        sys.create_keyspace(TEST_KS, 1)
        sys.create_column_family(TEST_KS, 'Standard1')
        sys.create_column_family(TEST_KS, 'Super1', super=True)
        sys.create_column_family(TEST_KS, 'Indexed1')
        sys.create_index(TEST_KS, 'Indexed1', 'birthdate', LONG_TYPE)
        if sys._conn.version != CASSANDRA_07:
            sys.create_column_family(TEST_KS, 'Counter1',
                                     default_validation_class=COUNTER_COLUMN_TYPE)
            sys.create_column_family(TEST_KS, 'SuperCounter1', super=True,
                                     default_validation_class=COUNTER_COLUMN_TYPE)
    except Exception, e:
        try:
            sys.drop_keyspace(TEST_KS)
        except:
            pass
        raise e
    sys.close()

def teardown_package():
    sys = SystemManager()
    if TEST_KS in sys.list_keyspaces():
        sys.drop_keyspace(TEST_KS)
    sys.close()
