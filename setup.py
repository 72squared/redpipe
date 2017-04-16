#!/usr/bin/env python
import os
from os import path
from setuptools import setup
import imp

NAME = 'rediswrap'

MYDIR = path.abspath(os.path.dirname(__file__))
long_description = open(os.path.join(MYDIR, 'README.md')).read()
version = imp.load_source(
    'version',
    path.join('.', 'rediswrap', 'version.py')).__version__

cmdclass = {}
ext_modules = []

setup(
    name='rediswrap',
    version=version,
    description='RedisWrap',
    author='John Loehrer',
    author_email='72squared@gmail.com',
    url='https://github.com/72squared/%s' % NAME,
    packages=[NAME],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Programming Language :: Python',
        'Programming Language :: Python',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Environment :: Web Environment',
        'Operating System :: POSIX',
    ],
    license='MIT',
    install_requires=[

    ],
    include_package_data=True,
    long_description=long_description,
    cmdclass=cmdclass,
    ext_modules=ext_modules
)
