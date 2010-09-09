#!/usr/bin/env python
# -*- coding: utf-8 -*-
#

__version_info__ = (0, 5, 0)
__version__ = '.'.join([str(v) for v in __version_info__])

"""pycassa is a Cassandra library with the following features:

1. Auto-failover single or thread-local connections
2. A simplified version of the thrift interface
3. A method to map an existing class to a Cassandra ColumnFamily.
4. Support for SuperColumns
"""

from distutils.core import setup
import sys

setup(
      name = 'pycassa',
      version = __version__,
      author = 'Jonathan Hseu',
      author_email = 'pycassa.maintainer@gmail.com',
      description = 'Simple python library for Cassandra',
      long_description = __doc__,
      url = 'http://github.com/pycassa/pycassa',
      download_url = 'http://github.com/pycassa/pycassa',
      license = 'MIT',
      keywords = 'cassandra client db distributed thrift',
      packages = ['pycassa', 'pycassa.cassandra'],
      platforms = 'any',
      install_requires = ['thrift'],
      scripts=['pycassaShell'],
      )
