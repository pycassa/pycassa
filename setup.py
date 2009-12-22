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
      license = 'BSD',
      keywords = 'cassandra client db distributed thrift',
      packages = ['pycasso', 'cassandra'],
      platforms = 'any',
      )
