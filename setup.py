#!/usr/bin/env python

"""Pog: a command line Azkaban client."""

from azkaban import __version__
from setuptools import setup

setup(
    name='azkaban',
    version=__version__,
    description='Azkaban CLI',
    long_description=open('README.rst').read(),
    author='Matthieu Monsch',
    author_email='monsch@alum.mit.edu',
    url='http://github.com/mtth/azkaban/',
    license='MIT',
    py_modules=['azkaban'],
    classifiers=[
      'Development Status :: 4 - Beta',
      'Intended Audience :: Developers',
      'License :: OSI Approved :: MIT License',
      'Programming Language :: Python',
    ],
    install_requires=[
      'docopt',
      'requests>=2.0.1',
    ],
    # entry_points={'console_scripts': []},
)
