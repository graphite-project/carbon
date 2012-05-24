#!/usr/bin/env python

import os
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
init_scripts = [ ('/etc/init.d', ['init/carbon-cache', 'init/carbon-relay', 'init/carbon-aggregator']) ]

setup(
  name='carbon',
  version='0.9.10_pre3',
  url='https://launchpad.net/graphite',
  author='Chris Davis',
  author_email='chrismd@gmail.com',
  license='Apache Software License 2.0',
  description='Backend data caching and persistence daemon for Graphite',
  packages=['carbon', 'carbon.aggregator', 'twisted.plugins'],
  package_dir={'' : 'lib'},
  scripts=glob('bin/*'),
  package_data={ 'carbon' : ['*.xml'] },
  data_files=storage_dirs + conf_files + init_scripts,
  install_requires=['twisted', 'txamqp'],
  **setup_kwargs
)
