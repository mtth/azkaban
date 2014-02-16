#!/usr/bin/env python
# encoding: utf-8

"""AzkabanPig: an extension for pig scripts to Azkaban CLI.

Usage:
  azkabanpig PATH ...
             [-j JAR] ... [-o OPTION] ...
             [-bp PROJECT] [-t TYPE] [-a ALIAS | -u URL]
  azkabanpig -h | --help

Arguments:
  PATH                          Path to pig script. If more than one path is
                                specified, they will be run in order. Note that
                                all the pig jobs will share the same Azkaban
                                options; for more flexibility, you can use the
                                Azkaban CLI directly.

Options:
  -a ALIAS --alias=ALIAS        Cf. `azkaban --help`.
  -b --background               Run job asynchronously. `azkabanpig` will
                                launch the workflow and return.
  -h --help                     Show this message and exit.
  -j JAR --jar=JAR              Path to jar file. It will be available on the
                                class path when the pig script is run, no need
                                to register it inside your scripts.
  -o OPTION --option=OPTION     Azkaban option. Should be of the form
                                key=value. E.g. '-o param.foo=bar' will
                                substitute parameter '$foo' with 'bar' in the
                                pig script.
  -p PROJECT --project=PROJECT  Project name under which to run the pig script.
  -t TYPE --type=TYPE           Pig job type used.
  -u URL --url=URL              Cf. `azkaban --help`.

Examples:
  azkabanpig my_script.pig
  azkabanpig -a foo first_script.pig second_script.pig
  azkabanpig -url http://url.to.azkaban -o param.my_output=bar.dat foo.pig

AzkabanPig returns with exit code 1 if an error occurred and 0 otherwise.

"""

__all__ = ['PigJob']

from docopt import docopt
from os import sep
from os.path import abspath, basename
from sys import stdout
from time import sleep
from ..job import Job
from ..project import Project
from ..session import Session
from ..util import AzkabanError, Config, catch, temppath


class PigJob(Job):

  """Job class corresponding to pig jobs.

  :param path: absolute path to pig script (this script will automatically be
    added to the project archive)
  :param options: cf. `Job`

  """

  #: Job type used (change this to use a custom pig type).
  type = 'pig'

  def __init__(self, path, *options):
    super(PigJob, self).__init__(
      {'type': self.type, 'pig.script': path.lstrip(sep)},
      *options
    )
    self.path = path

  def on_add(self, project, name):
    """This handler adds the corresponding script file to the project."""
    project.add_file(self.path)


class PigProject(Project):

  """Project to run pig jobs from the command line.

  :param name: project name used
  :param paths: paths to pig scripts, these will be run in order
  :param type: pig job type used
  :param jars: jars to include, these will also be added to the classpath
  :param options: options forwarded to pig scripts

  """

  def __init__(self, name, paths, user, type='pig', jars=None, options=None):
    super(PigProject, self).__init__(name, register=False)
    jars = jars or []
    opts = [
      {
        'type': type,
        'user.to.proxy': user,
        'pig.additional.jars': ','.join(abspath(j).lstrip(sep) for j in jars),
      },
      options or {}
    ]
    for jar in jars:
      self.add_file(abspath(jar))
    for path, dep in zip(paths, [None] + paths):
      dep_opts = {'dependencies': basename(dep)} if dep else {}
      self.add_job(basename(path), PigJob(abspath(path), dep_opts, *opts))


def _get_status(session, exec_id):
  """Get status of a PigProject execution, along with currently running job.

  :param session: `azkaban.session.Session`
  :param exec_id: execution ID

  This method is able to simply find which unique job is active because of
  the linear structure of the workflow.

  """
  status = session.get_execution_status(exec_id)
  active = [
    e['id']
    for e in status['nodes']
    if e['status'] == 'RUNNING' or e['status'] == 'FAILED'
  ]
  return {
    'flow_status': status['status'],
    'active_job': active[0] if active else '',
  }

@catch(AzkabanError)
def main():
  """AzkabanPig entry point."""
  args = docopt(__doc__)
  config = Config()
  paths = args['PATH']
  pj = args['--project'] or config.get_default_option('azkabanpig', 'project')
  tpe = args['--type'] or config.get_default_option('azkabanpig', 'type')
  session = Session(args['--url'], args['--alias'])
  try:
    job_options = dict(opt.split('=', 1) for opt in args['--option'])
  except ValueError:
    raise AzkabanError('Invalid options: %r' % (' '.join(args['OPTION']), ))
  project = PigProject(
    name=pj,
    paths=paths,
    user=session.user,
    type=tpe,
    jars=args['--jar'],
    options=job_options,
  )
  with temppath() as tpath:
    project.build(tpath)
    while True:
      try:
        session.upload_project(project, tpath)
      except AzkabanError:
        session.create_project(project, project)
      else:
        break
  res = session.run_workflow(project, basename(paths[-1]))
  exec_id = res['execid']
  if args['--background']:
    if len(paths) == 1:
      stdout.write(
        'Pig job running at %s/executor?execid=%s&job=%s\n'
        % (session.url, exec_id, basename(paths[0]))
      )
    else:
      stdout.write(
        'Pig jobs running at %s/executor?execid=%s\n'
        % (session.url, exec_id)
      )
  else:
    current_job = None
    try:
      while True:
        sleep(5)
        status = _get_status(session, exec_id)
        if status['active_job'] != current_job:
          current_job = status['active_job']
          if current_job:
            offset = 0
            stdout.write('\n[Job %s]\n' % (current_job, ))
        if current_job:
          logs = session.get_job_logs(
            exec_id=exec_id,
            job=current_job,
            offset=offset,
          )
          stdout.write(logs['data'])
          offset += logs['length']
        if status['flow_status'] == 'SUCCEEDED':
          stdout.write('\nExecution succeeded.\n')
          break
        elif status['flow_status'] != 'RUNNING':
          raise AzkabanError('Execution failed.')
    except KeyboardInterrupt:
      choice = raw_input('\nKill execution [yN]? ').lower()
      if choice and choice[0] == 'y':
        stdout.write('Killing... ')
        session.cancel_execution(exec_id)
        stdout.write('Done.\n')
      else:
        stdout.write(
          'Execution still running at %s/executor?execid=%s\n'
          % (session.url, exec_id)
        )

if __name__ == '__main__':
  main()
