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
four pig scripts.

.. code-block:: python

  from azkaban import PigJob, Project
  from getpass import getuser

  PROJECT = Project('sample', root=__file__)

  # properties available to all jobs
  PROJECT.properties = {
    'user.to.proxy': getuser(),
    'param': {
      'input_root': 'sample_dir/',
      'n_reducers': 20,
    },
    'jvm.args.mapred': {
      'max.split.size': 2684354560,
      'min.split.size': 2684354560,
    },
  }

  # list of pig jobs
  JOBS = [
    {'pig.script': 'first.pig'},
    {'pig.script': 'second.pig', 'dependencies': 'first.pig'},
    {'pig.script': 'third.pig', 'param': {'foo': 48}},
    {'pig.script': 'fourth.pig', 'dependencies': 'second.pig,third.pig'},
  ]

  for option in JOBS:
    PROJECT.add_job(option['pig.script'], PigJob(option))

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
