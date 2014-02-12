#!/usr/bin/env python
# encoding: utf-8

"""AzkabanPig: an extension for pig scripts to Azkaban CLI.

Usage:
  azkabanpig [-p PROJECT] [-t TYPE] (-u URL | -a ALIAS) PATH [OPTION ...]
  azkabanpig -h | --help

Arguments:
  PATH                          Path to pig script.
  OPTION                        Azkaban option. Should be of the form
                                key=value. E.g. 'param.foo=bar' will substitute
                                parameter '$foo' with 'bar' in the pig script.

Options:
  -a ALIAS --alias=ALIAS        Cf. `azkaban --help`.
  -h --help                     Show this message and exit.
  -p PROJECT --project=PROJECT  Project name under which to run the pig script
                                [default: pig_${user}].
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
      {
        'type': pig_type,
        'user.to.proxy': getuser(),
      },
      options or {},
    )
    self.add_job(basename(path), job)


def main():
  """AzkabanPig entry point."""
  args = docopt(__doc__)
  path = args['PATH']
  job_name = basename(path)
  project_name = Template(args['--project']).substitute(user=getuser())
  try:
    try:
      job_options = dict(opt.split('=', 1) for opt in args['OPTION'])
    except ValueError:
      raise AzkabanError('Invalid options: %r' % (' '.join(args['OPTION']), ))
    project = PigProject(project_name, path, args['--type'], job_options)
    session = project.get_session(url=args['--url'], alias=args['--alias'])
    with temppath() as tpath:
      project.build(tpath)
      try:
        project.upload(tpath, session['url'], session['session_id'])
      except AzkabanError:
        project.create(project.name, session['url'], session['session_id'])
        project.upload(tpath, session['url'], session['session_id'])
    res = project.run(job_name, session['url'], session['session_id'])
    exec_id = res['execid']
    stdout.write(
      'Pig job running at %s/executor?execid=%s&job=%s\n'
      % (session['url'], exec_id, job_name)
    )
  except AzkabanError as err:
    stderr.write('%s\n' % (err, ))
    exit(1)

if __name__ == '__main__':
  main()
