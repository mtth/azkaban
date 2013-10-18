Azkaban
=======

Command line interface for Azkaban_:

* Define all your jobs from a single python file
* Build project archives and upload to Azkaban
* And more...


Installation
------------

.. code:: bash

  $ pip install azkaban


Quickstart
----------

We first create a file to define our project. Let's call it :code:`jobs.py`, 
although any name would work. In this example, we add a single job and file to 
our project:

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
files.

.. code:: bash

  $ python jobs.py build foo.zip


More
----

A :code:`PigJob` class is provided, which automatically sets the job type and 
adds the corresponding script file to the project.

.. code:: python

  from azkaban import PigJob

  project.add_job('baz', PigJob('/.../baz.pig', {'dependencies': 'bar'}))


.. _Azkaban: http://data.linkedin.com/opensource/azkaban
