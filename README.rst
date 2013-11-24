Azkaban
=======

Lightweight command line interface (CLI) for Azkaban_:

* Define jobs from a single python file.
* Build projects and upload to Azkaban from the command line.


Installation
------------

Using pip_:

.. code:: bash

  $ pip install azkaban


Quickstart
----------

We first create a configuration file for our project. Let's call it 
:code:`jobs.py`, although any name would work. Here's a simple example of how 
we could define a project with a single job and static file:

.. code:: python

  from azkaban import Job, Project

  project = Project('foo')
  project.add_file('/path/to/bar.txt', 'bar.txt')
  project.add_job('bar', Job({'type': 'command', 'command': 'cat bar.txt'}))

  if __name__ == '__main__':
    project.main()

The :code:`add_file` method adds a file to the project archive (the second 
optional argument specifies the destination path inside the zip file). The 
:code:`add_job` method will trigger the creation of a :code:`.job` file. The 
first argument will be the file's name, the second is a :code:`Job` instance 
(cf. `Job options`_).

Once we've saved our jobs file, the following commands are available to us:

* :code:`python jobs.py list`, see the list of all jobs in the current 
  project.
* :code:`python jobs.py view`, view the contents of the :code:`.job` file for 
  a given job.
* :code:`python jobs.py build`, build the project archive and store it 
  locally.
* :code:`python jobs.py upload`, build and upload the project to an Azkaban 
  server.

Running :code:`python jobs.py --help` shows the list of options for each of 
the previous commands.


Job options
-----------

The :code:`Job` class is a light wrapper which allows the creation of 
:code:`.job` files using python dictionaries.

It also provides a convenient way to handle options shared across multiple 
jobs: the constructor can take in multiple options dictionaries and the last 
definition of an option (i.e. later in the arguments) will take precedence 
over earlier ones.

We can use this to efficiently share default options among jobs, for example:

.. code:: python

  defaults = {'user.to.proxy': 'boo', 'retries': 0}

  jobs = [
    Job({'type': 'noop'}),
    Job(defaults, {'type': 'noop'}),
    Job(defaults, {'type': 'command', 'command': 'ls'}),
    Job(defaults, {'type': 'command', 'command': 'ls -l', 'retries': 1}),
  ]

All jobs except the first one will have their :code:`user.to.proxy` property 
set. Note also that the last job overrides the :code:`retries` property.

Alternatively, if we really don't want to pass the defaults dictionary around, 
we can create a new :code:`Job` subclass to do it for us:

.. code:: python

  class BooJob(Job):

    def __init__(self, *options):
      super(BooJob, self).__init__(defaults, *options)


More
----

Aliases
*******

To avoid having to enter the server's URL and our username on every upload (or 
hard-coding it into our project's configuration file, ugh), we can define 
aliases in :code:`~/.azkabanrc`:

.. code:: cfg

  [foo]
  url = http://url.to.foo.server:port
  [bar]
  url = http://url.to.bar.server
  user = baruser

We can now upload directly to each of these URLs with the shorthand:

.. code:: bash

  $ python jobs.py upload -a foo

This has the added benefit that we won't have to authenticate on every upload. 
The session ID is cached and reused for later connections.


Nested options
**************

Nested dictionaries can be used to group options concisely:

.. code:: python

  # e.g. this job
  Job({
    'proxy.user': 'boo',
    'proxy.keytab.location': '/path',
    'param.input': 'foo',
    'param.output': 'bar',
  })
  # is equivalent to this one
  Job({
    'proxy': {'user': 'boo', 'keytab.location': '/path'},
    'param': {'input': 'foo', 'output': 'bar'},
  })


Pig jobs
********

Because pig jobs are so common, a :code:`PigJob` class is provided which 
accepts a file path (to the pig script) as first constructor argument, 
optionally followed by job options. It then automatically sets the job type 
and adds the corresponding script file to the project.

.. code:: python

  from azkaban import PigJob

  project.add_job('baz', PigJob('/.../baz.pig', {'dependencies': 'bar'}))

Using a custom pig type is as simple as changing the :code:`PigJob.type` class 
variable.

Merging Projects
****************

If you have multiple projects, you can merge them together to create a single project.
The merge is done in place on the project the method is called on. The first project will
retain its original name.

.. code:: python

  from azkaban import Job, Project

  project1 = Project('foo')
  project1.add_file('/path/to/bar.txt', 'bar.txt')
  project1.add_job('bar', Job({'type': 'command', 'command': 'cat bar.txt'}))

  project2 = Project('qux')
  project2.add_file('/path/to/baz.txt', 'baz.txt')
  project2.add_job('baz', Job({'type': 'command', 'command': 'cat baz.txt'}))

  # project1 will now contain baz.txt and the baz job from project2
  project1.merge(project2)

  if __name__ == '__main__':
    project1.main()

Next steps
**********

Any valid python code can go inside the jobs configuration file. This includes 
using loops to add jobs, subclassing the base :code:`Job` class to better suit 
a project's needs (e.g. by implementing the :code:`on_add` and 
:code:`on_build` handlers), ...


.. _Azkaban: http://data.linkedin.com/opensource/azkaban
.. _pip: http://www.pip-installer.org/en/latest/
