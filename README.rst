.. default-role:: code

Azkaban
=======

A lightweight Azkaban_ client providing:

* A `command line interface`_ to run jobs, upload projects, and much more.

  .. code:: bash

    $ azkaban upload my_project.zip
    Project my_project successfully uploaded (id: 1, size: 205kB, version: 1).
    Details at https://azkaban.server.url/manager?project=my_project

    $ azkaban run my_workflow
    Flow my_workflow successfully submitted (execution id: 1).
    Details at https://azkaban.server.url/executor?execid=1

* A simple syntax_ to define workflows from a single python file.

  .. code:: python

    from azkaban import Job, Project

    project = Project('my_project')
    project.add_file('/path/to/bar.txt', 'bar.txt')
    project.add_job('bar', Job({'type': 'command', 'command': 'cat bar.txt'}))


Installation
------------

Using pip_:

.. code:: bash

  $ pip install azkaban


Command line interface
----------------------

Once installed, the `azkaban` executable provides the following 
commands:

.. code:: bash

  azkaban (create | delete) [options]
  azkaban run [options] FLOW [JOB ...]
  azkaban upload [options] ZIP

Running `azkaban --help` shows the full list of options.


URLs and aliases
****************

The previous commands all take a `--url`, or `-u`, option used to specify 
where to find the Azkaban server (and which user to connect as).

.. code:: bash

  $ azkaban create -u http://url.to.foo.server:port

In order to avoid having to input the entire URL every time, it is possible to 
defines aliases in `~/.azkabanrc`:

.. code:: cfg

  [azkaban]
  default.alias = foo
  [alias]
  foo = http://url.to.foo.server:port
  bar = baruser@http://url.to.bar.server

We can now interact directly with each of these URLs using the `--alias`, or 
`-a` option followed by their corresponding alias. Since we also specified a 
default alias, it is also possible to omit the option altogether. As a result,
the commands below are all equivalent:

.. code:: bash

  $ azkaban create -u http://url.to.foo.server:port
  $ azkaban create -a foo
  $ azkaban create

Finally, our session ID for a given URL is cached on each successful login, so 
that we don't have to authenticate on every remote interaction.


Examples
********

* Creating and deleting projects:

  .. code:: bash

    $ azkaban create
    Project name: my_project
    Description [my_project]: Some interesting description.
    Project my_project successfully created.
    Details at https://azkaban.server.url/manager?project=my_project

    $ azkaban delete -a bar
    Project name: my_project
    Project my_project successfully deleted.

* Uploading an already built archive to an Azkaban server:

  .. code:: bash

    $ azkaban upload -p my_project my_project.zip

* Run entire workflows, or individual jobs:

  .. code:: bash

    $ azkaban run -p my_project my_workflow


Syntax
------

For medium to large sized projects, it quickly becomes tricky to manage the 
multitude of files required for each workflow. `.properties` files are 
helpful but still do not provide the flexibility to generate jobs 
programmatically (i.e. using `for` loops, etc.). This approach also 
requires us to manually bundle and upload our project to the gateway every 
time.

We provide here a convenient framework to define jobs from a single python 
file. This framework is entirely compatible with the command line interface 
above, and even provides additional functionality (e.g. building and uploading 
projects in a single command).


Quickstart
**********

We start by creating a configuration file for our project. Let's call it 
`jobs.py`, the default file name the command line tool will look for. 
Here's a simple example of how we could define a project with a single job and 
static file:

.. code:: python

  from azkaban import Job, Project

  project = Project('foo')
  project.add_file('/path/to/bar.txt', 'bar.txt')
  project.add_job('bar', Job({'type': 'command', 'command': 'cat bar.txt'}))

The `add_file` method adds a file to the project archive (the second 
optional argument specifies the destination path inside the zip file). The 
`add_job` method will trigger the creation of a `.job` file. The 
first argument will be the file's name, the second is a `Job` instance 
(cf. `Job options`_).

Once we've saved our jobs file, the following additional commands are 
available to us:

* `azkaban list`, see the list of all jobs in the current project.
* `azkaban view`, view the contents of the `.job` file for a given 
  job.
* `azkaban build`, build the project archive and store it locally.


Job options
***********

The `Job` class is a light wrapper which allows the creation of 
`.job` files using python dictionaries.

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

All jobs except the first one will have their `user.to.proxy` property 
set. Note also that the last job overrides the `retries` property.

Alternatively, if we really don't want to pass the defaults dictionary around, 
we can create a new `Job` subclass to do it for us:

.. code:: python

  class BooJob(Job):

    def __init__(self, *options):
      super(BooJob, self).__init__(defaults, *options)


More
****

Nested options
^^^^^^^^^^^^^^

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


Merging projects
^^^^^^^^^^^^^^^^

If you have multiple projects, you can merge them together to create a single 
project. The merge is done in place on the project the method is called on. 
The first project will retain its original name.

.. code:: python

  from azkaban import Job, Project

  project1 = Project('foo')
  project1.add_file('/path/to/bar.txt', 'bar.txt')
  project1.add_job('bar', Job({'type': 'command', 'command': 'cat bar.txt'}))

  project2 = Project('qux')
  project2.add_file('/path/to/baz.txt', 'baz.txt')
  project2.add_job('baz', Job({'type': 'command', 'command': 'cat baz.txt'}))

  # project1 will now contain baz.txt and the baz job from project2
  project2.merge_into(project1)


Job details
^^^^^^^^^^^

The `info` command becomes quite powerful when combined with other Unix 
tools. Here are a few examples:

.. code:: bash

  $ # To count the number of jobs per type
  $ azkaban info -o type | cut -f 2 | sort | uniq -c
  $ # To only view the list of jobs of a certain type with their dependencies
  $ azkaban info -o type,dependencies | awk -F '\t' '($2 == "job_type")'
  $ # To view the size of each file in the project
  $ azkaban info -f | xargs -n 1 du -h


Next steps
^^^^^^^^^^

Any valid python code can go inside the jobs configuration file. This includes 
using loops to add jobs, subclassing the base `Job` class to better suit 
a project's needs (e.g. by implementing the `on_add` and 
`on_build` handlers), ...


Extensions
----------

Pig
***

Because pig jobs are so common, a `PigJob` class is provided which 
accepts a file path (to the pig script) as first constructor argument, 
optionally followed by job options. It then automatically sets the job type 
and adds the corresponding script file to the project.

.. code:: python

  from azkaban import PigJob

  project.add_job('baz', PigJob('/.../baz.pig', {'dependencies': 'bar'}))

Using a custom pig type is as simple as changing the `PigJob.type` class 
variable.

This extension also comes with the `azkabanpig` executable to run pig scripts 
directly. `azkabanpig --help` will display the list of available options 
(using UDFs, substituting parameters, running several scripts in order, etc.).


.. _Azkaban: http://data.linkedin.com/opensource/azkaban
.. _pip: http://www.pip-installer.org/en/latest/
