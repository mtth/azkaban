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

  project.build('foo.zip')


.. _Azkaban: http://data.linkedin.com/opensource/azkaban
