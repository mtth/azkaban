#!/usr/bin/env python
# encoding: utf-8

"""AzkabanPig: an extension for pig scripts to Azkaban CLI.

Usage:
  azkabanpig PATH ...
             [-f FILE] ... [-j JAR] ... [-o OPTION] ...
             [-bp PROJECT] [-t TYPE] [-a ALIAS | -u URL]
  azkabanpig -h | --help | -l | --log

Arguments:
  PATH                          Path to pig script. If more than one path is
                                specified, they will be run in order. Note that
                                all the pig jobs will share the same Azkaban
                                options; for more flexibility, you can use
                                AzkabanCLI directly.

Options:
  -a ALIAS --alias=ALIAS        Cf. `azkaban --help`.
  -b --background               Run job asynchronously. AzkabanPig will launch
                                the workflow and return.
  -f FILE --file=FILE           Path of file to include when uploading.
  -h --help                     Show this message and exit.
  -j JAR --jar=JAR              Path to jar file. It will be available on the
                                class path when the pig script is run, no need
                                to register it inside your scripts.
  -l --log                      Show path to current log file and exit.
  -o OPTION --option=OPTION     Azkaban option. Can either be a path to a
                                properties file or a single option formatted as
                                key=value. E.g. `-o param.foo=bar` will
                                substitute parameter `$foo` with `'bar'` in the
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
from os import getcwd, pardir, sep
from os.path import abspath, basename, exists, isabs, relpath
from time import sleep
from ..job import Job
from ..project import Project
from ..remote import Execution, Session
from ..util import (AzkabanError, Config, catch, suppress_urllib_warnings,
temppath)
import logging as lg
import sys


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
      {'type': Config().get_option('azkabanpig', 'default.type', 'pig')},
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

  def on_add(self, project, name, **kwargs):
    """This handler adds the corresponding script file to the project."""
    if not 'merging' in kwargs:
      project.add_file(self.path)


class _PigProject(Project):

  """Project to run pig jobs from the command line.

  :param name: Project name used.
  :param paths: Paths to pig scripts, these will be run in order.
  :param pig_type: Pig job type used.

  """

  def __init__(self, name, paths, pig_type=None):
    super(_PigProject, self).__init__(name, register=False, root=getcwd())
    self.ordered_jobs = [basename(path) for path in paths]
    for path, dep in zip(paths, [None] + self.ordered_jobs):
      options = {'pig.script': abspath(path)}
      if pig_type:
        options['type'] = pig_type
      options['dependencies'] = dep or '' # override key if exists
      self.add_job(basename(path), PigJob(options))

  def logs(self, execution, delay=10):
    """Jobs logs. In order.

    :param execution: `azkaban.remote.Execution`
    :param delay: Poll delay (in seconds) while job is preparing.

    """
    ok_statuses = set(['RUNNING', 'SUCCEEDED'])
    for job in self.ordered_jobs:
      while execution.status['status'] == 'PREPARING':
        # Delay log query until job is done preparing otherwise the log file
        # won't exist yet (and the server will send a 500 back).
        self._logger.warning('Job %s preparing.', job)
        sleep(delay)
      for line in execution.job_logs(job):
        yield line
      if not execution.status['status'] in ok_statuses:
        raise AzkabanError('Job %s failed.', job)
      else:
        self._logger.info('Job %s finished.', job)


@catch(AzkabanError)
def main():
  """AzkabanPig entry point."""
  args = docopt(__doc__)
  cfg = Config()
  # activate logging
  logger = lg.getLogger()
  logger.setLevel(lg.DEBUG)
  handler = cfg.get_file_handler('azkabanpig')
  if handler:
    logger.addHandler(handler)
  # capture pesky unverified requests warnings
  suppress_urllib_warnings()
  # handle this command separately
  if args['--log']:
    if handler:
      sys.stdout.write('%s\n' % (handler.baseFilename, ))
      sys.exit(0)
    else:
      raise AzkabanError('No log file active.')
  # create project
  paths = args['PATH']
  jars = args['--jar'] or []
  if args['--url']:
    session = Session(url=args['--url'], config=cfg)
  else:
    alias = args['--alias'] or cfg.get_option('azkabanpig', 'default.alias')
    session = Session.from_alias(alias=alias, config=cfg)
  project = _PigProject(
    name=args['--project'] or cfg.get_option('azkabanpig', 'default.project'),
    paths=paths,
    pig_type=args['--type'],
  )
  project.properties = {
    'user.to.proxy': session.user,
    'pig.additional.jars': ','.join(abspath(j).lstrip(sep) for j in jars),
  }
  project.properties.update(
    opt.split('=', 1)
    for opt in args['--option']
    if '=' in opt
  )
  job_paths = (opt for opt in args['--option'] if not '=' in opt)
  for i, job_path in enumerate(job_paths):
    if not exists(job_path):
      raise AzkabanError(
        'Invalid `--option`: %s\nOptions should point to an existing job file'
        ' or be of the form `key=value`.'
        % (job_path, )
      )
    project.add_file(abspath(job_path), '%s.properties' % (i, ))
  for fpath in args['--file']:
    if exists(fpath) and not isabs(fpath) and pardir in relpath(fpath):
      # We give a more useful error message than what `add_file` would raise.
      # For simplicity, AzkabanPig doesn't allow directly configuring the
      # archive path of included files (in the vast majority of cases, a
      # relative include is much more convenient than absolute, so we can't
      # just use the absolute path).
      raise AzkabanError(
        'Included files must either be below the CWD or be included using an '
        'absolute path.\nUse an absolute path to include %s'
        % (fpath, )
      )
    project.add_file(fpath)
  for jar in jars:
    project.add_file(abspath(jar))
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
    sys.stdout.write('Execution running at %s\n' % (exe.url, ))
  else:
    try:
      for line in project.logs(exe):
        sys.stdout.write('%s\n' % (line.encode('utf-8'), ))
    except KeyboardInterrupt:
      choice = raw_input('\nCancel execution [yN]? ').lower()
      if choice and choice[0] == 'y':
        sys.stdout.write('Killing... ')
        exe.cancel()
        sys.stdout.write('Done.\n')
      else:
        sys.stdout.write('Execution still running at %s\n' % (exe.url, ))

if __name__ == '__main__':
  main()
