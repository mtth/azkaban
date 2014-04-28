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
  -b --background               Run job asynchronously. AzkabanPig will launch
                                the workflow and return.
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
  azkabanpig -ba foo first_script.pig second_script.pig
  azkabanpig -u http://url.to.azkaban -o param.my_output=bar.dat foo.pig

AzkabanPig returns with exit code 1 if an error occurred and 0 otherwise.

"""

__all__ = ['PigJob']

from docopt import docopt
from os import sep
from os.path import abspath, basename
from sys import stdout
from ..job import Job
from ..project import Project
from ..remote import Execution, Session
from ..util import AzkabanError, Config, catch, temppath


class PigJob(Job):

  """Convenience job class for running pig scripts.

  :param options: Tuple of options (cf. :class:`~azkaban.job.Job`). These
    options must specify a `'pig.script'` key. The corresponding file will
    then automatically be included in the project archive.

  This class allows you to specify JVM args as a dictionary by correctly
  converting these to the format used by Azkaban when building the job options.
  For example: `{'jvm.args': {'foo': 1, 'bar': 2}}` will be converted to
  `jvm.args=-Dfoo=1 -Dbar=2`. Note that this enables JVM args to behave like
  all other `Job` options when defined multiple times (latest values taking
  precedence).

  Finally, by default the job type will be set automatically to `'pig'`. You
  can also specify a custom job type for all :class:`PigJob` instances in the
  `azkabanpig` section of the `~/.azkabanrc` configuration file via the
  `default.type` option.

  """

  def __init__(self, *options):
    super(PigJob, self).__init__(
      {'type': Config().get_option('azkabanpig', 'type', 'pig')},
      *options
    )
    try:
      self.path = self.options['pig.script']
    except KeyError:
      raise AzkabanError('Missing `\'pig.script\'` option.')
    else:
      # absolute archive paths get trimmed
      self.options['pig.script'] = self.path.lstrip('/')
      self.join_prefix('jvm.args', ' ', '-D%s=%s')

  def on_add(self, project, name):
    """This handler adds the corresponding script file to the project."""
    project.add_file(self.path, self.path)


class _PigProject(Project):

  """Project to run pig jobs from the command line.

  :param name: project name used
  :param paths: paths to pig scripts, these will be run in order
  :param type: pig job type used
  :param jars: jars to include, these will also be added to the classpath
  :param options: options forwarded to pig scripts

  """

  def __init__(self, name, paths, user, type=None, jars=None, options=None):
    super(_PigProject, self).__init__(name, register=False)
    self.ordered_jobs = [basename(path) for path in paths]
    jars = jars or []
    default_options = {
      'user.to.proxy': user,
      'pig.additional.jars': ','.join(abspath(j).lstrip(sep) for j in jars),
    }
    if type:
      default_options['type'] = type
    for jar in jars:
      self.add_file(abspath(jar))
    for path, dep in zip(paths, [None] + self.ordered_jobs):
      dependency_options = {'dependencies': dep} if dep else {}
      self.add_job(
        basename(path),
        PigJob(
          {'pig.script': abspath(path)},
          default_options,
          dependency_options,
          options or {},
        )
      )

  def logs(self, execution):
    """Jobs logs. In order.

    :param execution: `azkaban.remote.Execution`

    """
    for job in self.ordered_jobs:
      for line in execution.job_logs(job):
        yield line
      if execution.status['status'] == 'FAILURE':
        raise AzkabanError('Execution failed.')


@catch(AzkabanError)
def main():
  """AzkabanPig entry point."""
  args = docopt(__doc__)
  paths = args['PATH']
  session = Session(args['--url'], args['--alias'])
  try:
    job_options = dict(opt.split('=', 1) for opt in args['--option'])
  except ValueError:
    raise AzkabanError('Invalid options: %r' % (' '.join(args['OPTION']), ))
  project = _PigProject(
    name=args['--project'] or Config().get_option('azkabanpig', 'project'),
    paths=paths,
    user=session.user,
    type=args['--type'],
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
  exe = Execution(session, res['execid'])
  if args['--background']:
    stdout.write('Execution running at %s\n' % (exe.url, ))
  else:
    try:
      for line in project.logs(exe):
        stdout.write('%s\n' % (line.encode('utf-8'), ))
    except KeyboardInterrupt:
      choice = raw_input('\nCancel execution [yN]? ').lower()
      if choice and choice[0] == 'y':
        stdout.write('Killing... ')
        exe.cancel()
        stdout.write('Done.\n')
      else:
        stdout.write('Execution still running at %s\n' % (exe.url, ))

if __name__ == '__main__':
  main()
