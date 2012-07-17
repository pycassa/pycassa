from pycassa.system_manager import SystemManager

TEST_KS = 'PycassaTestKeyspace'

def setup_package():
    sys = SystemManager()
    if TEST_KS in sys.list_keyspaces():
        sys.drop_keyspace(TEST_KS)
    try:
        sys.create_keyspace(TEST_KS, 'SimpleStrategy', {'replication_factor': '1'})
        sys.create_column_family(TEST_KS, 'Standard1')
        sys.create_column_family(TEST_KS, 'Super1', super=True)
        sys.create_column_family(TEST_KS, 'Indexed1')
        sys.create_index(TEST_KS, 'Indexed1', 'birthdate', 'LongType')
        sys.create_column_family(TEST_KS, 'Counter1',
                                 default_validation_class='CounterColumnType')
        sys.create_column_family(TEST_KS, 'SuperCounter1', super=True,
                                 default_validation_class='CounterColumnType')
    except Exception, e:
        print e
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
