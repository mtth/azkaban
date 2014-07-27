Quickstart
==========

Command line interface
----------------------

Overview
********

Once installed, the `azkaban` executable provides several useful commands. 
These are divided into two kinds. The first will work out of the box with any 
existing Azkaban project:

* `azkaban run [options] WORKFLOW [JOB ...]`

  Launch (asynchronously) an entire workflow or specific jobs in a given 
  workflow. This command will print the corresponding execution's URL to 
  standard out.

* `azkaban upload [options] ZIP`

  Upload an existing project zip archive.

* `azkaban log [options] EXECUTION [JOB]`

  View execution logs. If the execution is still running, the command will 
  return on completion.

The second require a project configuration file (cf. `building projects`_):

* `azkaban build [options]`

  Generate a project's job files and package them in a zip file along with any 
  other project dependencies (e.g. jars,  pig scripts). This archive can 
  either be saved to disk or directly uploaded to Azkaban.

* `azkaban info [options]`

  View information about all the jobs inside a project, its static 
  dependencies, or a specific job's options.

Running `azkaban --help` shows the full list of options available for each 
command.


URLs and aliases
****************

The previous commands all take a `--url`, option used to specify where to find 
the Azkaban server (and which user to connect as).

.. code-block:: bash

  $ azkaban create -u http://url.to.foo.server:port

In order to avoid having to input the entire URL every time, it is possible to 
defines aliases in `~/.azkabanrc`:

.. code-block:: cfg

  [alias]
  foo = http://url.to.foo.server:port
  bar = baruser@http://url.to.bar.server
  [azkaban]
  default.alias = foo

We can now interact directly with each of these URLs using the `--alias` 
option followed by their corresponding alias. Since we also specified a 
default alias, it is also possible to omit the option altogether. As a result,
the commands below are all equivalent:

.. code-block:: bash

  $ azkaban create -u http://url.to.foo.server:port
  $ azkaban create -a foo
  $ azkaban create

Note finally that our session ID is cached on each successful login, so that 
we won't have to authenticate on every remote interaction.


Building projects
-----------------

We provide here a framework to define projects, jobs, and workflows from a 
single python file.


Motivation
**********

For medium to large sized projects, it quickly becomes tricky to manage the 
multitude of files required for each workflow. `.properties` files are helpful 
but still do not provide the flexibility to generate jobs programmatically 
(i.e. using `for` loops, etc.). This approach also requires us to manually 
bundle and upload our project to the gateway every time.

Additionally, this will enable the `build` and `info` commands.


Quickstart
**********

We start by creating a file. Let's call it `jobs.py` (the default file name 
the command line tool will look for), although any name would work. Below is a 
simple example of how we could define a project with a single job and static 
file:

.. code-block:: python

  from azkaban import Job, Project

  project = Project('foo')
  project.add_file('/path/to/bar.txt', 'bar.txt')
  project.add_job('bar', Job({'type': 'command', 'command': 'cat bar.txt'}))

The :class:`~azkaban.project.Project` class corresponds transparently to a 
project on the Azkaban server. The :meth:`~azkaban.project.Project.add_file` 
method then adds a file to the project archive (the second optional argument 
specifies the destination path inside the zip file). Similarly, the 
:meth:`~azkaban.project.Project.add_job` method will trigger the creation of a 
`.job` file. The first argument will be the file's name, the second is a 
:class:`~azkaban.job.Job` instance (cf. `Job options`_).

Once we've saved our jobs file, running the `azkaban` executable in the same 
directory will pick it up automatically and activate all commands. Note that we 
could also specify a custom configuration file location with the `-p --project` 
option (e.g. if the jobs file was in a different location).


Job options
***********

The :class:`~azkaban.job.Job` class is a light wrapper which allows the 
creation of `.job` files using python dictionaries.

It also provides a convenient way to handle options shared across multiple 
jobs: the constructor can take in multiple options dictionaries and the last 
definition of an option (i.e. later in the arguments) will take precedence 
over earlier ones.

We can use this to efficiently share default options among jobs, for example:

.. code-block:: python

  defaults = {'user.to.proxy': 'foo', 'retries': 0}

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

.. code-block:: python

  class FooJob(Job):

    def __init__(self, *options):
      super(FooJob, self).__init__(defaults, *options)

Finally, since many Azkaban options are space- or comma-separated strings (e.g. 
dependencies), the :class:`~azkaban.job.Job` class provides helpers to 
conveniently transform python objects into these: 
:meth:`~azkaban.job.Job.join_option` and :meth:`~azkaban.job.Job.join_prefix`.


More
****

Nested options
^^^^^^^^^^^^^^

Nested dictionaries can be used to group options concisely:

.. code-block:: python

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

.. code-block:: python

  from azkaban import Job, Project

  project1 = Project('foo')
  project1.add_file('/path/to/bar.txt', 'bar.txt')
  project1.add_job('bar', Job({'type': 'command', 'command': 'cat bar.txt'}))

  project2 = Project('qux')
  project2.add_file('/path/to/baz.txt', 'baz.txt')
  project2.add_job('baz', Job({'type': 'command', 'command': 'cat baz.txt'}))

  # project1 will now contain baz.txt and the baz job from project2
  project2.merge_into(project1)


Next steps
**********

Any valid python code can go inside a jobs configuration file. This includes 
using loops to add jobs, subclassing the base `Job` class to better suit a 
project's needs (e.g. by implementing the `on_add` handler), etc.

Finally, the `info` command becomes quite powerful when combined with other 
Unix tools. Here are a few examples:

* Counting the number of jobs per type: `azkaban info -o type | cut -f 2 | 
  sort | uniq -c`

* Viewing the list of jobs of a certain type, along with their dependencies: 
  `azkaban info -o type,dependencies | awk -F '\t' '($2 == "job_type")'`

* Viewing the size of each file in the project: `azkaban info -f | xargs -n 1 
  du -h`
