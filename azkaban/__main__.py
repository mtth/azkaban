#!/usr/bin/env python
# encoding: utf-8

"""Azkaban CLI: a lightweight command line interface for Azkaban.

Usage:
  azkaban build [-cp PROJECT] [-a ALIAS | -u URL | [-r] ZIP] [-o OPTION ...]
  azkaban info [-p PROJECT] [-f | -o OPTION ... | [-i] JOB ...]
  azkaban log [-a ALIAS | -u URL] EXECUTION [JOB]
  azkaban run [-jkp PROJECT] [-a ALIAS | -u URL] [-b | -m MODE] [-e EMAIL ...]
              [-o OPTION ...] FLOW [JOB ...]
  azkaban schedule [-jknp PROJECT] [-a ALIAS | -u URL] [-b | -m MODE]
                   [-e EMAIL ...] [-o OPTION ...] [-s SPAN] (-d DATE) (-t TIME)
                   FLOW [JOB ...]
  azkaban upload [-cp PROJECT] [-a ALIAS | -u URL] ZIP
  azkaban -h | --help | -l | --log | -v | --version

Commmands:
  build*                        Build project and upload to Azkaban or save
                                locally the resulting archive.
  info*                         View information about jobs or files.
  log                           View workflow or job execution logs.
  run                           Run jobs or workflows. If no job is specified,
                                the entire workflow will be executed.
  schedule                      Schedule a workflow to be run at a specified
                                date and time.
  upload                        Upload archive to Azkaban server.

Arguments:
  EXECUTION                     Execution ID.
  JOB                           Job name.
  FLOW                          Workflow name. Recall that in the Azkaban world
                                this is simply a job without children.
  ZIP                           For `upload` command, the path to an existing
                                project zip archive. For `build`, the path
                                where the output archive will be built. If it
                                points to a directory, the archive will be
                                named after the project name (and version, if
                                present) and created in said directory.

Options:
  -a ALIAS --alias=ALIAS        Alias to saved URL and username. Will also try
                                to reuse session IDs for later connections.
  -b --bounce                   Skip execution if workflow is already running.
                                Shortcut for `--mode=skip`.
  -c --create                   Create the project if it does not exist.
  -d DATE --date=DATE           Date used for first run of a schedule. It must
                                be in the format `MM/DD/YYYY`.
  -e EMAIL --email=EMAIL        Email address to be notified when the workflow
                                finishes (can be specified multiple times).
  -f --files                    List project files instead of jobs. The first
                                column is the local path of the file, the
                                second the path of the file in the archive.
  -h --help                     Show this message and exit.
  -i --include-properties       Include project properties with job options.
  -j --jump                     Skip any specified jobs instead of only running
                                those.
  -k --kill                     Kill worfklow on first job failure.
  -l --log                      Show path to current log file and exit.
  -m MODE --mode=MODE           Concurrency mode. The default is to allow
                                concurrent executions. See also `--bounce`.
  -n --notify_early             Send any notification emails when the first job
                                fails rather than when the entire workflow
                                finishes.
  -o OPTION --option=OPTION     Azkaban properties. Can either be the path to
                                a properties file or a single parameter
                                formatted as `key=value`, e.g. `-o
                                user.to.proxy=foo`. For the `build` and `run`
                                commands, these will be added to the project's
                                or run's properties respectively (potentially
                                overriding existing ones). For the `info`
                                command, this will cause only jobs with these
                                exact parameters to be displayed.
  -p PROJECT --project=PROJECT  Azkaban project. Can either be a project name
                                or a path to a python module/package defining
                                an `azkaban.Project` instance. Commands which
                                are followed by an asterisk will only work in
                                the latter case. If multiple projects are
                                registered, you can disambiguate as follows:
                                `--project=module:project_name`.
  -r --replace                  Overwrite any existing file.
  -s SPAN --span=SPAN           Period to repeat the scheduled flow. Must be
                                in format `1d`, a combination of magnitude and
                                unit of repetition. If not specified, the flow
                                will be run only once.
  -t TIME --time=TIME           Time when a schedule should be run. Must be of
                                the format `hh,mm,(AM|PM),(PDT|UTC|..)`.
  -u URL --url=URL              Azkaban endpoint (with protocol, and optionally
                                a username): '[user@]protocol:endpoint'. E.g.
                                'http://azkaban.server'. The username defaults
                                to the current user, as determined by `whoami`.
                                If you often use the same url, consider using
                                the `--alias` option instead.
  -v --version                  Show version and exit.

Azkaban CLI returns with exit code 1 if an error occurred and 0 otherwise.

"""

from azkaban import __version__, CLI_ARGS
from azkaban.project import Project
from azkaban.remote import Execution, Session
from azkaban.util import (AzkabanError, Config, catch, flatten, human_readable,
temppath, read_properties, suppress_urllib_warnings, write_properties)
from docopt import docopt
from traceback import format_exc
from requests.exceptions import HTTPError
import logging as lg
import os
import os.path as osp
import sys


_logger = lg.getLogger(__name__)


def _forward(args, names):
  """Forward subset of arguments from initial dictionary.

  :param args: Dictionary of parsed arguments (output of `docopt.docopt`).
  :param names: List of names that will be included.

  """
  names = set(names)
  return dict(
    ('_%s' % (k.lower().lstrip('-').replace('-', '_'), ), v)
    for (k, v) in args.items() if k in names
  )

def _parse_option(_option):
  """Parse `--option` argument.

  :param _option: `--option` argument.

  Returns a dictionary.

  """
  paths = (opt for opt in _option if not '=' in opt)
  opts = read_properties(*paths)
  try:
    opts.update(dict(s.split('=', 1) for s in _option if '=' in s))
  except ValueError:
    raise AzkabanError('Invalid `--option` flag.')
  return opts

def _parse_project(_project, require_project=False):
  """Parse `--project` argument into `(name, project)`.

  :param _project: `--project` argument.
  :param require_project: Fail if we fail to load the project.

  Note that `name` is guaranteed to be non-`None` (this function will throw an
  exception otherwise) but `project` can be.

  The rules are as follows:

  + If at least one `':'` is found in `_project` then the rightmost one is
    interpreted as delimitor between the path to the module and the project
    name.

  + Else:

    + We first try to interpret `_project` as a module path and find a unique
      project inside.

    + If the above attempt raises an `ImportError`, we interpret it as a name.

  """
  default_project = Config().get_option('azkaban', 'default.project', 'jobs')
  exceptions = {}
  projects = {}

  def try_load(path):
    try:
      projects.update(Project.load(path))
      return True
    except Exception:
      exceptions[path] = format_exc()
      return False

  _project = _project or default_project
  if ':' in _project:
    # unambiguous case
    path, name = _project.rsplit(':', 1)
    if ':' in default_project:
      try_load(Project.load(path))
    else:
      # adding the default here lets options like `-p :name` work as intended
      try_load(path or default_project)
  else:
    # the option could be a name or module
    if not try_load(_project): # try first as a module
      # if that fails, try as a name
      name = _project
      if not ':' in default_project:
        path = default_project
      else:
        path = default_project.rsplit(':', 1)[0]
        # if the default project could be a mdule, try loading it
      try_load(path)
    else:
      name = None
      path = _project

  if exceptions:
    footer = '\nErrors occurred while loading the following modules:\n'
    for t in exceptions.items():
      footer += '\n> %r\n\n%s' % t
  else:
    footer = ''

  if name:
    if name in projects:
      return name, projects[name]
    elif projects:
      # harder consistency requirement
      raise AzkabanError(
        'Project %r not found. Available projects: %s\n'
        'You can also specify another location using the `--project` option.'
        '%s'
        % (name, ', '.join(projects), footer)
      )
    elif require_project:
      raise AzkabanError(
        'This command requires a project configuration module.\n'
        'You can specify another location using the `--project` option.'
        '%s'
        % (footer, )
      )
    else:
      return name, None
  else:
    if not projects:
      raise AzkabanError(
        'No registered project found in %r.\n'
        'You can specify another location using the `--project` option.'
        '%s'
        % (path, footer)
      )
    elif len(projects) > 1:
      raise AzkabanError(
        'Multiple registered projects found: %s\n'
        'You can use the `--project` option to disambiguate.'
        '%s'
        % (', '.join(projects), footer)
      )
    else:
      return projects.popitem()

def _get_project_name(_project):
  """Return project name.

  :param _project: `--project` argument.

  """
  return _parse_project(_project)[0]

def _load_project(_project):
  """Resolve project from CLI argument.

  :param _project: `--project` argument.

  """
  try:
    name, project = _parse_project(_project, require_project=True)
  except ImportError:
    raise AzkabanError(
      'This command requires a project configuration module which was not '
      'found.\nYou can specify another location using the `--project` option.'
    )
  else:
    return project

def _get_session(url, alias):
  """Get appropriate session.

  :param url: URL (has precedence over alias).
  :param alias: Alias name.

  """
  config = Config()
  if url:
    return Session(url=url, config=config)
  else:
    alias = alias or config.get_option('azkaban', 'default.alias')
    return Session.from_alias(alias=alias, config=config)

def _upload_zip(session, name, path, create=False, archive_name=None):
  """Upload zip to project in Azkaban.

  :param session: Remote Azkaban session.
  :param name: Project name
  :param path: Path to zip file.
  :param create: Create project if it doesn't exist.
  :param archive_name: Optional zip file name (used by Azkaban).

  """

  def _callback(cur_bytes, tot_bytes, file_index, _stdout=sys.stdout):
    """Callback for streaming upload.

    :param cur_bytes: Total bytes uploaded so far.
    :param tot_bytes: Total bytes to be uploaded.
    :param file_index: (0-based) index of the file currently uploaded.
    :param _stdout: Performance caching.

    """
    if cur_bytes != tot_bytes:
      _stdout.write(
        'Uploading project: %.1f%%\r'
        % (100. * cur_bytes / tot_bytes, )
      )
    else:
      _stdout.write('Validating project...    \r')
    _stdout.flush()

  while True:
    try:
      res = session.upload_project(
        name=name,
        path=path,
        archive_name=archive_name,
        callback=_callback,
      )
    except AzkabanError as err:
      if create and str(err).endswith("doesn't exist."):
        session.create_project(name, name)
      else:
        raise err
    except HTTPError as err:
      # See https://github.com/mtth/azkaban/pull/34 for more context on why the
      # logic below is necessary.
      code = err.response.status_code
      if code == 400:
        raise AzkabanError(
          "Failed to upload project (%s HTTP error). Check that the project "
          "is not locked, exists, and that your user has write permissions."
          % (code, )
        )
      elif code == 401:
        raise AzkabanError(
          "Not authorized to upload project (%s HTTP error). Check that your "
          "user has write permissions." % (code, )
        )
      elif code == 410:
        session.create_project(name, name)
      else:
        raise err
    else:
      return res

def view_info(project, _files, _option, _job, _include_properties):
  """List jobs in project."""
  if _job:
    if _include_properties:
      write_properties(
        flatten(project.properties),
        header='project.properties'
      )
    for name in _job:
      project.jobs[name].build(header='%s.job' % (name, ))
  elif _files:
    for path, archive_path in sorted(project.files):
      sys.stdout.write('%s\t%s\n' % (osp.relpath(path), archive_path))
  else:
    options = _parse_option(_option).items()
    jobs = sorted(project.jobs.items())
    dependencies = set(
      dep.strip()
      for _, job in jobs
      for dep in job.options.get('dependencies', '').split(',')
    )
    for name, job in jobs:
      if all(job.options.get(k) == v for k, v in options):
        sys.stdout.write(
          '%s\t%s\n'
          % ('J' if name in dependencies else 'F', name, )
        )

def view_log(_execution, _job, _url, _alias):
  """View workflow or job execution logs."""
  session = _get_session(_url, _alias)
  exc = Execution(session, _execution)
  logs = exc.job_logs(_job[0]) if _job else exc.logs()
  try:
    for line in logs:
      sys.stdout.write('%s\n' % (line.encode('utf-8'), ))
  except HTTPError:
    # Azkaban responds with 500 if the execution or job isn't found
    if _job:
      raise AzkabanError(
        'Execution %s and/or job %s not found.', _execution, _job
      )
    else:
      raise AzkabanError('Execution %s not found.', _execution)

def run_workflow(project_name, _flow, _job, _url, _alias, _bounce, _kill,
  _email, _option, _jump, _mode):
  """Run workflow."""
  session = _get_session(_url, _alias)
  kwargs = {
    'name': project_name,
    'flow': _flow,
    'concurrent': _mode if _mode else not _bounce,
    'on_failure': 'cancel' if _kill else 'finish',
    'emails': _email,
    'properties': _parse_option(_option),
  }
  if _jump:
    kwargs['disabled_jobs'] = _job
  else:
    kwargs['jobs'] = _job
  res = session.run_workflow(**kwargs)
  exec_id = res['execid']
  job_names = ', jobs: %s' % (', '.join(_job), ) if _job else ''
  sys.stdout.write(
    'Flow %s successfully submitted (execution id: %s%s).\n'
    'Details at %s/executor?execid=%s\n'
    % (_flow, exec_id, job_names, session.url, exec_id)
  )

def schedule_workflow(project_name, _date, _time, _span, _flow, _job, _url,
  _alias, _bounce, _kill, _email, _option, _jump, _notify_early, _mode):
  """Schedule workflow."""
  session = _get_session(_url, _alias)
  kwargs = {
    'name': project_name,
    'flow': _flow,
    'date': _date,
    'time': _time,
    'period': _span,
    'concurrent': _mode if _mode else not _bounce,
    'on_failure': 'cancel' if _kill else 'finish',
    'emails': _email,
    'properties': _parse_option(_option),
  }
  if _notify_early:
    kwargs['notify_early'] = True
  if _jump:
    kwargs['disabled_jobs'] = _job
  else:
    kwargs['jobs'] = _job
  res = session.schedule_workflow(**kwargs)
  sys.stdout.write(
    'Flow %s scheduled successfully.\n' % (_flow, )
  )

def upload_project(project_name, _zip, _url, _alias, _create):
  """Upload project."""
  session = _get_session(_url, _alias)
  res = _upload_zip(session, project_name, _zip, _create)
  sys.stdout.write(
    'Project %s successfully uploaded (id: %s, size: %s, version: %s).\n'
    'Details at %s/manager?project=%s\n'
    % (
      project_name,
      res['projectId'],
      human_readable(osp.getsize(_zip)),
      res['version'],
      session.url,
      project_name,
    )
  )

def build_project(project, _zip, _url, _alias, _replace, _create, _option):
  """Build project."""
  if _option:
    project.properties = flatten(project.properties)
    # to make sure we properly override nested options, we flatten first
    project.properties.update(_parse_option(_option))
  if _zip:
    if osp.isdir(_zip):
      _zip = osp.join(_zip, '%s.zip' % (project.versioned_name, ))
    project.build(_zip, overwrite=_replace)
    sys.stdout.write(
      'Project %s successfully built and saved as %r (size: %s).\n'
      % (project, _zip, human_readable(osp.getsize(_zip)))
    )
  else:
    with temppath() as _zip:
      project.build(_zip)
      archive_name = '%s.zip' % (project.versioned_name, )
      session = _get_session(_url, _alias)
      res = _upload_zip(session, project.name, _zip, _create, archive_name)
      sys.stdout.write(
        'Project %s successfully built and uploaded '
        '(id: %s, size: %s, upload: %s).\n'
        'Details at %s/manager?project=%s\n'
        % (
          project,
          res['projectId'],
          human_readable(osp.getsize(_zip)),
          res['version'],
          session.url,
          project,
        )
      )

@catch(AzkabanError)
def main(argv=None):
  """Entry point."""
  # enable general logging
  logger = lg.getLogger()
  logger.setLevel(lg.DEBUG)
  handler = Config().get_file_handler('azkaban')
  if handler:
    logger.addHandler(handler)
  # capture pesky unverified requests warnings
  suppress_urllib_warnings()
  # parse arguments
  argv = argv or sys.argv[1:]
  _logger.debug('Running command %r from %r.', ' '.join(argv), os.getcwd())
  args = docopt(__doc__, version=__version__)
  CLI_ARGS.update(args)
  # do things
  if args['--log']:
    if handler:
      sys.stdout.write('%s\n' % (handler.baseFilename, ))
    else:
      raise AzkabanError('No log file active.')
  elif args['build']:
    build_project(
      _load_project(args['--project']),
      **_forward(
        args,
        ['ZIP', '--url', '--alias', '--replace', '--create', '--option']
      )
    )
  elif args['log']:
    view_log(
      **_forward(args, ['EXECUTION', 'JOB', '--url', '--alias'])
    )
  elif args['info']:
    view_info(
      _load_project(args['--project']),
      **_forward(args, ['--files', '--option', 'JOB', '--include-properties'])
    )
  elif args['run']:
    run_workflow(
      _get_project_name(args['--project']),
      **_forward(
        args,
        [
          'FLOW', 'JOB', '--bounce', '--url', '--alias', '--kill', '--email',
          '--option', '--jump', '--mode',
        ]
      )
    )
  elif args['schedule']:
    schedule_workflow(
      _get_project_name(args['--project']),
      **_forward(
        args,
        [
          'FLOW', 'JOB', '--bounce', '--url', '--alias', '--kill', '--email',
          '--option', '--date', '--time', '--span', '--jump', '--notify_early', '--mode',
        ]
      )
    )
  elif args['upload']:
    upload_project(
      _get_project_name(args['--project']),
      **_forward(args, ['ZIP', '--create', '--url', '--alias'])
    )

if __name__ == '__main__':
  main()
