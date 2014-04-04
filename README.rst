.. default-role:: code


AzkabanCLI |build_image|
------------------------

.. |build_image| image:: https://travis-ci.org/mtth/azkaban.png?branch=master
  :target: https://travis-ci.org/mtth/azkaban

A lightweight Azkaban_ client providing:

* A command line interface to run workflows, upload projects, etc.

  .. code-block:: bash

    $ azkaban upload my_project.zip

    Project my_project successfully uploaded (id: 1, size: 205kB, version: 1).
    Details at https://azkaban.server.url/manager?project=my_project

* A convenient and extensible way for building projects:

  .. code-block:: python

    from azkaban import Job, Project

    project = Project('my_project')
    project.add_file('hey.txt')
    project.add_job('hi', Job({'type': 'command', 'command': 'cat hey.txt'}))


Documentation
-------------

The full documentation can be found here_ along with a few examples_.


Installation
------------

Using pip_:

.. code-block:: bash

  $ pip install azkaban


.. _Azkaban: http://data.linkedin.com/opensource/azkaban
.. _pip: http://www.pip-installer.org/en/latest/
.. _here: http://azkabancli.readthedocs.org/
.. _examples: https://github.com/mtth/azkaban/tree/master/examples
