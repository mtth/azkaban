Azkaban
=======

Lightweight command line interface (CLI) for Azkaban_:

* Define jobs from a single python file
* Build projects and upload to Azkaban from the command line

Integration is meant to be as transparent as possible: no additional folders 
and files and no imposed project structure.


Installation
------------

Using pip_:

.. code:: bash

  $ pip install azkaban


Quickstart
----------

We first create a file to define our project. Let's call it :code:`jobs.py`, 
although any name would work.

In this example, we add a single job and file:

.. code:: python

  from azkaban import Job, Project

  project = Project('foo')

  project.add_job('bar', Job({'type': 'command', 'command': 'echo "Hello!"'}))
  project.add_file('bax.jar')

  if __name__ == '__main__':
    project.main()

From the command line we can now run :code:`python jobs.py --help` to view the 
list of all available options. E.g. the following command will create the 
archive :code:`foo.zip` containing all the project's jobs and dependency 
files:

.. code:: bash

  $ python jobs.py build foo.zip


More
----

Aliases
*******

To avoid having to enter the server's URL on every upload (or hard-coding it 
into our project configuration file, ugh), we can define an alias in 
:code:`~/.azkabanrc`:

.. code:: cfg

  [foo]
  url = http://url.to.server:port

We can now upload directly to this URL with the command:

.. code:: bash

  $ python jobs.py upload -a foo

This has the added benefit that we won't have to authenticate on every upload. 
The session ID is cached and reused for later connections.


Pig jobs
********

A :code:`PigJob` class is provided, which automatically sets the job type and 
adds the corresponding script file to the project.

.. code:: python

  from azkaban import PigJob

  project.add_job('baz', PigJob('/.../baz.pig', {'dependencies': 'bar'}))


.. _Azkaban: http://data.linkedin.com/opensource/azkaban
.. _pip: http://www.pip-installer.org/en/latest/
