from functools import wraps

from nose.plugins.skip import SkipTest

def requireOPP(f):
    """ Decorator to require an order-preserving partitioner """

    @wraps(f)
    def wrapper(self, *args, **kwargs):
        partitioner = self.sys_man.describe_partitioner()
        if partitioner in ('RandomPartitioner', 'Murmur3Partitioner'):
            raise SkipTest('Must use order preserving partitioner for this test')
        return f(self, *args, **kwargs)

    return wrapper
