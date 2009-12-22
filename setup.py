#!/usr/bin/env python
# -*- coding: utf-8 -*-
#

"""pycasso is a Cassandra library with the following features:

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

from pycasso import __version__

setup(
      name = 'pycasso',
      version = __version__,
      author = 'Jonathan Hseu',
      author_email = 'vomjom@vomjom.net',
      description = 'Simple python library for Cassandra',
      long_description = __doc__,
      url = 'http://github.com/vomjom/pycasso',
      download_url = 'http://github.com/vomjom/pycasso',
      license = 'MIT',
      keywords = 'cassandra client db distributed thrift',
      packages = ['pycasso']+optional_packages,
      platforms = 'any',
      )
