#!/usr/bin/env python
# encoding: utf-8

"""AzkabanPig: an extension for pig scripts to Azkaban CLI.

Usage:
  azkabanpig [-p PROJECT] [-t TYPE] (-u URL | -a ALIAS) PATH [(-- OPTION ...)]
  azkabanpig -h | --help

Arguments:
  PATH                          Path to pig script.
  OPTION                        Options forwarded to pig.

Options:
  -a ALIAS --alias=ALIAS        Cf. `azkaban --help`.
  -h --help                     Show this message and exit.
  -p PROJECT --project=PROJECT  Project name [default: pig_${user}].
  -t TYPE --type=TYPE           Pig job type used [default: pig].
  -u URL --url=URL              Cf. `azkaban --help`.

"""

from docopt import docopt
from getpass import getuser
from os.path import abspath, basename
from string import Template
from sys import exit, stderr, stdout
from ..job import PigJob
from ..project import Project
from ..util import AzkabanError, temppath


class PigProject(Project):

  """Project to run a single pig job from the command line.

  :param path: path to a pig script
  :param pig_type: pig job type used
  :param options: options forwarded to pig script

  """

  def __init__(self, name, path, pig_type='pig', options=None):
    super(PigProject, self).__init__(name, register=False)
    job = PigJob(
      abspath(path),
      {'type': pig_type, 'jvm.args': ' '.join(options) if options else ''},
    )
    self.add_job(basename(path), job)


def main():
  """AzkabanPig entry point."""
  args = docopt(__doc__)
  path = args['PATH']
  name = Template(args['--project']).substitute(user=getuser())
  try:
    project = PigProject(name, path, args['--type'], args['OPTION'])
    session = project.get_session(url=args['--url'], alias=args['--alias'])
    with temppath() as tpath:
      project.build(tpath)
      try:
        project.upload(tpath, session['url'], session['session_id'])
      except AzkabanError:
        project.create(project.name, session['url'], session['session_id'])
        project.upload(tpath, session['url'], session['session_id'])
    res = project.run(basename(path), session['url'], session['session_id'])
    exec_id = res['execid']
    stdout.write(
      'Pig job successfully submitted.\n'
      'Details at %s/executor?execid=%s\n'
      % (session['url'], exec_id)
    )
  except AzkabanError as err:
    stderr.write('%s\n' % (err, ))
    exit(1)

if __name__ == '__main__':
  main()
