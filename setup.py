#!/usr/bin/env python
# -*- coding: utf-8 -*-
#

import sys
import os

try:
    import subprocess
    has_subprocess = True
except:
    has_subprocess = False

from ez_setup import use_setuptools
use_setuptools()
from setuptools import setup
from distutils.cmd import Command

__version_info__ = (0, 5, 1)
__version__ = '.'.join([str(v) for v in __version_info__])

long_description = """pycassa is a Cassandra library with the following features:

1. Auto-failover single or thread-local connections
2. A simplified version of the thrift interface
3. A method to map an existing class to a Cassandra ColumnFamily.
4. Support for SuperColumns
"""

class doc(Command):

    description = "generate or test documentation"

    user_options = [("test", "t",
                     "run doctests instead of generating documentation")]

    boolean_options = ["test"]

    def initialize_options(self):
        self.test = False

    def finalize_options(self):
        pass

    def run(self):
        if self.test:
            path = "doc/_build/doctest"
            mode = "doctest"
        else:
            path = "doc/_build/%s" % __version__
            mode = "html"

            try:
                os.makedirs(path)
            except:
                pass

        if has_subprocess:
            status = subprocess.call(["sphinx-build", "-b", mode, "doc", path])

            if status:
                raise RuntimeError("documentation step '%s' failed" % mode)

            print ""
            print "Documentation step '%s' performed, results here:" % mode
            print "   %s/" % path
        else:
            print """
`setup.py doc` is not supported for this version of Python.

Please ask in the user forums for help.
"""


setup(
      name = 'pycassa',
      version = __version__,
      author = 'Jonathan Hseu',
      author_email = 'pycassa.maintainer@gmail.com',
      description = 'Simple python library for Cassandra',
      long_description = long_description,
      url = 'http://github.com/pycassa/pycassa',
      download_url = 'http://github.com/pycassa/pycassa',
      license = 'MIT',
      keywords = 'cassandra client db distributed thrift',
      packages = ['pycassa', 'pycassa.cassandra'],
      platforms = 'any',
      install_requires = ['thrift'],
      scripts=['pycassaShell'],
      cmdclass={"doc": doc}
      )
