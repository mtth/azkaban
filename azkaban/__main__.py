#!/usr/bin/env python
# encoding: utf-8

"""Azkaban CLI: a lightweight command line interface for Azkaban.

Usage:
  azkaban build [-cp PROJECT] [-a ALIAS | -u URL | [-r] ZIP] [-o OPTION ...]
  azkaban info [-p PROJECT] [-f | -o OPTION ... | [-i] JOB ...]
  azkaban log [-a ALIAS | -u URL] EXECUTION [JOB]
  azkaban run [-bkp PROJECT] [-a ALIAS | -u URL] [-e EMAIL ...]
              [-o OPTION ...] FLOW [JOB ...]
  azkaban schedule [-bkp PROJECT] [-a ALIAS | -u URL] [-e EMAIL ...]
                   [-o OPTION ...] (-d DATE) (-t TIME) [-s SPAN]
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
  -k --kill                     Kill worfklow on first job failure.
  -l --log                      Show path to current log file and exit.
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

from azkaban import __version__
from azkaban.project import Project
from azkaban.remote import Execution, Session
from azkaban.util import (AzkabanError, Config, catch, flatten, human_readable,
  temppath, read_properties, write_properties)
from docopt import docopt
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
  default_module = Config().get_option('azkaban', 'project', 'jobs')
  projects = {}
  _project = _project or default_module
  if ':' in _project:
    # unambiguous case
    path, name = _project.rsplit(':', 1)
    try:
      projects = Project.load(path or default_module)
      # adding the default here lets options like `-p :name` work as intended
    except ImportError:
      pass
  else:
    # the option could be a name or module
    try:
      # try first as a module
      projects = Project.load(_project)
    except ImportError:
      # if that fails, try as a name: load the default module and look there
      name = _project
      try:
        projects = Project.load(default_module)
      except ImportError:
        pass
    else:
      name = None
  if name:
    if name in projects:
      return name, projects[name]
    elif projects:
      # harder consistency requirement
      raise AzkabanError(
        'Project %r not found. Available projects: %s\n'
        'You can also specify another location using the `--project` option.'
        % (name, ', '.join(projects))
      )
    elif require_project:
      raise AzkabanError(
        'This command requires a project configuration module.\n'
        'You can specify another location using the `--project` option.'
      )
    else:
      return name, None
  else:
    if not projects:
      raise AzkabanError(
        'No registered project found in %r.\n'
        'You can also specify another location using the `--project` option.'
        % (_project, )
      )
    elif len(projects) > 1:
      raise AzkabanError(
        'Multiple registered projects found: %s\n'
        'You can use the `--project` option to disambiguate.'
        % (', '.join(projects), )
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

def _upload_callback(cur_bytes, tot_bytes, file_index, _stdout=sys.stdout):
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
      dep
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
  session = Session(_url, _alias)
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
  _email, _option):
  """Run workflow."""
  session = Session(_url, _alias)
  res = session.run_workflow(
    name=project_name,
    flow=_flow,
    jobs=_job,
    concurrent=not _bounce,
    on_failure='cancel' if _kill else 'finish',
    emails=_email,
    properties=_parse_option(_option),
  )
  exec_id = res['execid']
  job_names = ', jobs: %s' % (', '.join(_job), ) if _job else ''
  sys.stdout.write(
    'Flow %s successfully submitted (execution id: %s%s).\n'
    'Details at %s/executor?execid=%s\n'
    % (_flow, exec_id, job_names, session.url, exec_id)
  )

def schedule_workflow(project_name, _date, _time, _span, _flow, _job, _url,
  _alias, _bounce, _kill, _email, _option):
  """Schedule workflow."""
  session = Session(_url, _alias)
  res = session.schedule_workflow(
    name=project_name,
    flow=_flow,
    date=_date,
    time=_time,
    period=_span,
    jobs=_job,
    concurrent=not _bounce,
    on_failure='cancel' if _kill else 'finish',
    emails=_email,
    properties=_parse_option(_option),
  )
  sys.stdout.write(
    'Flow %s scheduled successfully.\n' % (_flow, )
  )

def upload_project(project_name, _zip, _url, _alias, _create):
  """Upload project."""
  session = Session(_url, _alias)
  while True:
    try:
      res = session.upload_project(
        name=project_name,
        path=_zip,
        callback=_upload_callback
      )
    except AzkabanError as err:
      if _create:
        session.create_project(project_name, project_name)
      else:
        raise err
    else:
      break
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
      session = Session(_url, _alias)
      while True:
        try:
          res = session.upload_project(
            name=project.name,
            path=_zip,
            archive_name=archive_name,
            callback=_upload_callback
          )
        except AzkabanError as err:
          if _create and str(err).endswith("doesn't exist."):
            session.create_project(project.name, project.name)
          else:
            raise err
        else:
          break
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
  # parse arguments
  argv = argv or sys.argv[1:]
  args = docopt(__doc__, version=__version__)
  _logger.debug('Running command %r from %r.', ' '.join(argv), os.getcwd())
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
          '--option',
        ]
      )
    )
  elif args['schedule']:
    schedule_workflow(
      _get_project_name(args['--project']),
      **_forward(
        args,
        [
          'FLOW', 'JOB', '--bounce', '--url', '--alias', '--kill',
          '--email', '--option', '--date', '--time', '--span'
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
