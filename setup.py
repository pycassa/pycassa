#!/usr/bin/env python
# -*- coding: utf-8 -*-
#

__version_info__ = (0, 3, 0)
__version__ = '.'.join([str(v) for v in __version_info__])

"""pycassa is a Cassandra library with the following features:

1. Auto-failover single or thread-local connections
2. A simplified version of the thrift interface
3. A method to map an existing class to a Cassandra ColumnFamily.
4. Support for SuperColumns
"""

from distutils.core import setup
import sys

optional_packages = []

flags = [('--cassandra', 'cassandra'),
         ('-cassandra', 'cassandra')]

for flag, package in flags:
    if flag in sys.argv:
        optional_packages.append(package)
        sys.argv.remove(flag)

setup(
      name = 'pycassa',
      version = __version__,
      author = 'Jonathan Hseu',
      author_email = 'vomjom@vomjom.net',
      description = 'Simple python library for Cassandra',
      long_description = __doc__,
      url = 'http://github.com/vomjom/pycassa',
      download_url = 'http://github.com/vomjom/pycassa',
      license = 'MIT',
      keywords = 'cassandra client db distributed thrift',
      packages = ['pycassa']+optional_packages,
      platforms = 'any',
      install_requires = ['thrift'],
      )
