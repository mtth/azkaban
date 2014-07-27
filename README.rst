.. default-role:: code


AzkabanCLI |build_image|
------------------------

.. |build_image| image:: https://travis-ci.org/mtth/azkaban.png?branch=master
  :target: https://travis-ci.org/mtth/azkaban

A lightweight Azkaban_ client providing:

* A command line interface to run workflows, upload projects, etc.
* A convenient and extensible way for building projects.


Sample
------

Below is a simple configuration file for a project containing a workflow with 
three jobs:

.. code-block:: python

  from azkaban import Job, Project
  from getpass import getuser

  PROJECT = Project('sample')

  # properties available to all jobs
  PROJECT.properties = {
    'user.to.proxy': getuser(),
  }

  # dictionary of jobs
  JOBS = {
    'first': Job({'type': 'command', 'command': 'echo "Hello"'}),
    'second': Job({'type': 'command', 'command': 'echo "World"'}),
    'third': Job({'type': 'noop', 'dependencies': 'first,second'}),
  }

  for name, job in JOBS.items():
    PROJECT.add_job(name, job)

The examples_ directory contains another sample project that uses Azkaban 
properties to build a project with two configurations: production and test, 
without any job duplication.


Documentation
-------------

The full documentation can be found here_.


Installation
------------

Using pip_:

.. code-block:: bash

  $ pip install azkaban


.. _Azkaban: http://data.linkedin.com/opensource/azkaban
.. _pip: http://www.pip-installer.org/en/latest/
.. _here: http://azkabancli.readthedocs.org/
.. _examples: https://github.com/mtth/azkaban/tree/master/examples
