#!/usr/bin/env python

"""AzkabanCLI: a lightweight command line interface for Azkaban."""

from azkaban import __version__
from setuptools import find_packages, setup


setup(
    name='azkaban',
    version=__version__,
    description=__doc__,
    long_description=open('README.rst').read(),
    author='Matthieu Monsch',
    author_email='monsch@alum.mit.edu',
    url='http://azkabancli.readthedocs.org/',
    license='MIT',
    packages=find_packages(),
    classifiers=[
      'Development Status :: 4 - Beta',
      'Intended Audience :: Developers',
      'License :: OSI Approved :: MIT License',
      'Programming Language :: Python',
      'Programming Language :: Python :: 2.6',
      'Programming Language :: Python :: 2.7',
      'Programming Language :: Python :: 3',
      'Programming Language :: Python :: 3.3',
    ],
    install_requires=[
      'six',
      'docopt',
      'requests>=2.0.1',
    ],
    entry_points={'console_scripts': [
      'azkaban = azkaban.__main__:main',
      'azkabanpig = azkaban.ext.pig:main',
    ]},
)
