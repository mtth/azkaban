Azkaban
=======

Command line interface for Azkaban_.

.. code:: python

  from azkaban import Job, Project

  project = Project('foo')

  project.add_job(
    'bar',
    Job({'type': 'command', 'command': 'echo "Hello, World!"'})
  )

  project.add_file('baz.jar')

  project.run()


Convenience job class for pig jobs:

.. code:: python

  from azkaban import PigJob

  project.add_job(
    'foobar',
    PigJob('foobar.pig', {'dependencies': 'bar'})
  )


.. _Azkaban: http://data.linkedin.com/opensource/azkaban
