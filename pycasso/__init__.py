__version_info__ = (0, 1)
__version__ = '.'.join([str(v) for v in __version_info__])

from pycasso.columnfamily import *
from pycasso.columnfamilymap import *
from pycasso.connection import *

from cassandra.ttypes import ConsistencyLevel, InvalidRequestException, \
    NotFoundException, UnavailableException, TimedOutException
