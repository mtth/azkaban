.. default-role:: code


AzkabanCLI |build_image|
------------------------

.. |build_image| image:: https://travis-ci.org/mtth/azkaban.png?branch=master
  :target: https://travis-ci.org/mtth/azkaban

A lightweight Azkaban_ client providing:

* A command line interface to run workflows, upload projects, etc.
* A convenient and extensible way for building projects.


Sample configuration file
-------------------------

.. code-block:: python

  from azkaban import PigJob, Project
  from getpass import getuser

  project = Project('sample', root=__file__)

  # default options for all scripts
  default_options = {
    'user.to.proxy': getuser(),
    'mapred': {
      'max.split.size': 2684354560,
      'min.split.size': 2684354560,
    },
  }

  # dictionary of pig script options, keyed on the pig script path
  pig_job_options = {
    'first.pig': {},
    'second.pig': {'dependencies': 'first.pig'},
    'third.pig': {'param': {'foo': 48, 'bar': 'abc'}},
    'fourth.pig': {'dependencies': 'second.pig,third.pig'},
  }

  for path, options in pig_job_options.items():
    project.add_job(path, PigJob(path, default_options, options))

More examples_ are also available.

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
