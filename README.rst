Azkaban
=======

Lightweight command line interface (CLI) for Azkaban_:

* Define jobs from a single python file
* Build projects and upload to Azkaban from the command line


Installation
------------

Using pip_:

.. code:: bash

  $ pip install azkaban


Quickstart
----------

We first create a configuration file for our project. Let's call it 
:code:`jobs.py`, although any name would work. Here's a simple example of how 
we could define a project with a single job and a static file:

.. code:: python

  from azkaban import Job, Project

  project = Project('foo')
  project.add_file('/path/to/bar.txt', 'bar.txt')
  project.add_job('bar', Job({'type': 'command', 'command': 'cat bar.txt'}))

  if __name__ == '__main__':
    project.main()

The :code:`add_file` method adds a file to the project archive (the second 
optional argument specifies the destination path inside the zip file).

The :code:`add_job` method will trigger the creation of a :code:`.job` file. 
The first method argument will be the file's name, the second is a :code:`Job` 
instance. :code:`Job` instances accept a dictionary as constructor argument 
which is then used to generate the contents of their job file.

From the command line we can now run :code:`python jobs.py --help` to view the 
list of all available options (:code:`build`, :code:`upload`, etc.). E.g. the 
following command will create the archive :code:`foo.zip` containing all the 
project's jobs and dependency files:

.. code:: bash

  $ python jobs.py build foo.zip


Job options
-----------

There often are options which are shared across multiple jobs. For this 
reason, the :code:`Job` constructor can take in multiple options dictionaries. 
The last definition of an option (i.e. later in the arguments) will take 
precedence over earlier ones.

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

To avoid having to enter the server's URL on every upload (or hard-coding it 
into our project's configuration file, ugh), we can define aliases in 
:code:`~/.azkabanrc`:

.. code:: cfg

  [foo]
  url = http://url.to.foo.server:port
  [bar]
  url = http://url.to.bar.server:port

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


Next steps
**********

Any valid python code can go inside the jobs configuration file. This includes 
using loops to add jobs, subclassing the base :code:`Job` class to better suit 
a project's needs (e.g. by implementing the :code:`on_add` and 
:code:`on_build` handlers), ...


.. _Azkaban: http://data.linkedin.com/opensource/azkaban
.. _pip: http://www.pip-installer.org/en/latest/
