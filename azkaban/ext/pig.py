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


from docopt import docopt
from getpass import getuser
from os import sep
from os.path import abspath, basename
from string import Template
from sys import exit, stderr, stdout
from time import sleep
from ..job import Job
from ..project import Project
from ..session import Session
from ..util import AzkabanError, temppath


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

  def get_status(self, exec_id, url, session_id):
    """Get status of an execution, along with currently running job.

    :param exec_id: execution ID
    :param url: Azkaban server endpoint
    :param session_id: valid session id

    This method is able to simply find which unique job is active because of
    the linear structure of the workflow.

    """
    status = get_execution_status(exec_id, url, session_id)
    active = [
      e['id']
      for e in status['nodes']
      if e['status'] == 'RUNNING' or e['status'] == 'FAILED'
    ]
    return {
      'flow_status': status['status'],
      'active_job': active[0] if active else '',
    }


def main():
  """AzkabanPig entry point."""
  args = docopt(__doc__)
  project_name = Template(args['--project']).substitute(user=getuser())
  paths = args['PATH']
  try:
    try:
      job_options = dict(opt.split('=', 1) for opt in args['--option'])
    except ValueError:
      raise AzkabanError('Invalid options: %r' % (' '.join(args['OPTION']), ))
    project = PigProject(
      name=project_name,
      paths=paths,
      user=None,# TODO
      pig_type=args['--type'],
      jars=args['--jar'],
      options=job_options,
    )
    session = get_session(url=args['--url'], alias=args['--alias'])
    url = session['url']
    session_id = session['session_id']
    with temppath() as tpath:
      project.build(tpath)
      try:
        project.upload(tpath, url, session_id)
      except AzkabanError as err:
        try:
          project.create(project.name, url, session_id)
        except AzkabanError:
          raise err
        else:
          project.upload(tpath, url, session_id)
    res = project.run(basename(paths[-1]), url, session_id)
    exec_id = res['execid']
    if args['--background']:
      if len(paths) == 1:
        stdout.write(
          'Pig job running at %s/executor?execid=%s&job=%s\n'
          % (session['url'], exec_id, basename(paths[0]))
        )
      else:
        stdout.write(
          'Pig jobs running at %s/executor?execid=%s\n'
          % (session['url'], exec_id)
        )
    else:
      current_job = None
      try:
        while True:
          sleep(5)
          status = project.get_status(exec_id, url, session_id)
          if status['active_job'] != current_job:
            current_job = status['active_job']
            if current_job:
              offset = 0
              stdout.write('\n[Job %s]\n' % (current_job, ))
          if current_job:
            logs = get_job_logs(
              exec_id=exec_id,
              url=url,
              session_id=session_id,
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
      except KeyboardInterrupt as err:
        choice = raw_input('\nKill execution [yN]? ').lower()
        if choice and choice[0] == 'y':
          stdout.write('Killing... ')
          cancel_execution(exec_id, url, session_id)
          stdout.write('Done.\n')
        else:
          stdout.write(
            'Execution still running at %s/executor?execid=%s\n'
            % (session['url'], exec_id)
          )
  except AzkabanError as err:
    stderr.write('%s\n' % (err, ))
    exit(1)

if __name__ == '__main__':
  main()
