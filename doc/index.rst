.. azkaban documentation master file, created by
   sphinx-quickstart on Thu Mar  6 16:04:56 2014.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.


AzkabanCLI
==========

A lightweight Azkaban_ client providing:

* A command line interface to run jobs, upload projects, and more.

  .. code-block:: bash

    $ azkaban --project=my_project upload archive.zip
    Project my_project successfully uploaded (id: 1, size: 205kB, version: 1).
    Details at https://azkaban.server.url/manager?project=my_project

    $ azkaban --project=my_project run my_flow
    Flow my_flow successfully submitted (execution id: 48).
    Details at https://azkaban.server.url/executor?execid=48

* A convenient and extensible way to build project configuration files.

  .. code-block:: python

    from azkaban import PigJob, Project
    from getpass import getuser

    PROJECT = Project('sample', root=__file__)

    # default options for all jobs
    DEFAULTS = {
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

    # list of pig job options
    OPTIONS = [
      {'pig.script': 'first.pig'},
      {'pig.script': 'second.pig', 'dependencies': 'first.pig'},
      {'pig.script': 'third.pig', 'param': {'foo': 48}},
      {'pig.script': 'fourth.pig', 'dependencies': 'second.pig,third.pig'},
    ]

    for option in OPTIONS:
      PROJECT.add_job(option['pig.script'], PigJob(DEFAULTS, option))


Table of contents
-----------------

.. toctree::
  :maxdepth: 2

  quickstart
  api
  extensions


Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`


.. _Azkaban: http://data.linkedin.com/opensource/azkaban
