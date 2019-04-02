# AzkabanCLI [![Build badge](https://travis-ci.org/mtth/azkaban.png?branch=master)](https://travis-ci.org/mtth/azkaban) [![Pypi badge](https://badge.fury.io/py/azkaban.svg)](https://pypi.python.org/pypi/azkaban/) [![Downloads badge](https://img.shields.io/pypi/dm/azkaban.svg)](https://pypistats.org/packages/azkaban)

A lightweight [Azkaban][] client providing:

* A command line interface to run workflows, upload projects, etc.
* A convenient and extensible way for building projects.

## Sample

Below is a simple configuration file for a project containing a workflow with 
three jobs:

```python
from azkaban import Job, Project
from getpass import getuser

PROJECT = Project('sample')

# properties available to all jobs
PROJECT.properties = {
  'user.to.proxy': getuser(),
}

# dictionary of jobs
JOBS = {
  'first': Job({'type': 'command', 'command': 'echo "Hello"'}),
  'second': Job({'type': 'command', 'command': 'echo "World"'}),
  'third': Job({'type': 'noop', 'dependencies': 'first,second'}),
}

for name, job in JOBS.items():
  PROJECT.add_job(name, job)
```

The [examples][] directory contains another sample project that uses Azkaban 
properties to build a project with two configurations: production and test, 
without any job duplication.

## Documentation

The full documentation can be found [here][doc].

## Installation

Using [pip][]:

```sh
$ pip install azkaban
```

## Development

Run tests:

```sh
$ nosetests
```

To also run the integration tests against an Azkaban server, create
`~/.azkabanrc` that includes at least:

```cfg
[azkaban]
test.alias = local

[alias.local]
url = azkaban:azkaban@http://localhost:8081
```

[Azkaban]: http://data.linkedin.com/opensource/azkaban
[doc]: http://azkabancli.readthedocs.org/
[examples]: https://github.com/mtth/azkaban/tree/master/examples
[pip]: http://www.pip-installer.org/en/latest/
