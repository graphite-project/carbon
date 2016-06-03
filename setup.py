#!/usr/bin/env python

import os
import platform
from glob import glob

if os.environ.get('USE_SETUPTOOLS'):
  from setuptools import setup
  setup_kwargs = dict(zip_safe=0)

else:
  from distutils.core import setup
  setup_kwargs = dict()


storage_dirs = [ ('storage/whisper',[]), ('storage/lists',[]),
                 ('storage/log',[]), ('storage/rrd',[]) ]
conf_files = [ ('conf', glob('conf/*.example')) ]

install_files = storage_dirs + conf_files

# Let's include redhat init scripts, despite build platform
# but won't put them in /etc/init.d/ automatically anymore
init_scripts = [ ('examples/init.d', ['distro/redhat/init.d/carbon-cache',
                                      'distro/redhat/init.d/carbon-relay',
                                      'distro/redhat/init.d/carbon-aggregator']) ]
install_files += init_scripts


setup(
  name='carbon',
  version='0.9.15',
  url='http://graphite-project.github.com',
  author='Chris Davis',
  author_email='chrismd@gmail.com',
  license='Apache Software License 2.0',
  description='Backend data caching and persistence daemon for Graphite',
  packages=['carbon', 'carbon.aggregator', 'twisted.plugins'],
  package_dir={'' : 'lib'},
  scripts=glob('bin/*'),
  package_data={ 'carbon' : ['*.xml'] },
  data_files=install_files,
  install_requires=['twisted', 'txamqp'],
  **setup_kwargs
)
