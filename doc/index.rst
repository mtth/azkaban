.. azkaban documentation master file, created by
   sphinx-quickstart on Thu Mar  6 16:04:56 2014.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.


AzkabanCLI
==========

A lightweight Azkaban_ client providing:

* A command line interface to run jobs, upload projects, and more.

  .. code-block:: bash

    $ azkaban upload my_project.zip

    Project my_project successfully uploaded (id: 1, size: 205kB, version: 1).
    Details at https://azkaban.server.url/manager?project=my_project

* A convenient and extensible way to build project configuration files.

  .. code-block:: python

    from azkaban import Job, Project

    project = Project('my_project')
    project.add_file('hey.txt')
    project.add_job('hi', Job({'type': 'command', 'command': 'cat hey.txt'}))


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
