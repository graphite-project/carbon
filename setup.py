#!/usr/bin/env python

import os
from glob import glob
from setuptools import setup


storage_dirs = [('storage/ceres/dummy.txt', []), ('storage/whisper/dummy.txt', []),
                ('storage/lists', []), ('storage/log/dummy.txt', []),
                ('storage/rrd/dummy.txt', [])]
conf_files = [('conf', glob('conf/*.example'))]

install_files = storage_dirs + conf_files

# Let's include redhat init scripts, despite build platform
# but won't put them in /etc/init.d/ automatically anymore
init_scripts = [('examples/init.d', ['distro/redhat/init.d/carbon-cache',
                                     'distro/redhat/init.d/carbon-relay',
                                     'distro/redhat/init.d/carbon-aggregator'])]
install_files += init_scripts


def read(fname):
    with open(os.path.join(os.path.dirname(__file__), fname)) as f:
        return f.read()


setup(
    name='carbon',
    version='1.2.0',
    url='http://graphiteapp.org/',
    author='Chris Davis',
    author_email='chrismd@gmail.com',
    license='Apache Software License 2.0',
    description='Backend data caching and persistence daemon for Graphite',
    long_description=read('README.md'),
    long_description_content_type='text/markdown',
    packages=['carbon', 'carbon.aggregator', 'twisted.plugins'],
    package_dir={'': 'lib'},
    scripts=glob('bin/*'),
    package_data={'carbon': ['*.xml']},
    data_files=install_files,
    install_requires=['Twisted', 'txAMQP', 'cachetools', 'urllib3'],
    classifiers=(
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
    ),
    zip_safe=False
)
