#!/usr/bin/env python
# encoding: utf-8

"""Azkaban CLI: a lightweight command line interface for Azkaban.

Usage:
  azkaban build [-cp PROJECT] [-a ALIAS | -u URL | [-r] ZIP]
  azkaban info [-p PROJECT] [-f | -o OPTIONS | [-i] JOB ...]
  azkaban log [-a ALIAS | -u URL] EXECUTION [JOB]
  azkaban run [-ksp PROJECT] [-a ALIAS | -u URL] [-e EMAILS] WORKFLOW [JOB ...]
  azkaban upload [-cp PROJECT] [-a ALIAS | -u URL] ZIP
  azkaban -h | --help | -l | --log | -v | --version

Commmands:
  build*                        Build project and upload to Azkaban or save
                                locally the resulting archive.
  info*                         View information about jobs or files.
  log                           View workflow or job execution logs.
  run                           Run jobs or workflows. If no job is specified,
                                the entire workflow will be executed.
  upload                        Upload archive to Azkaban server.

Arguments:
  EXECUTION                     Execution ID.
  JOB                           Job name.
  WORKFLOW                      Workflow name. Recall that in the Azkaban world
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
  -c --create                   Create the project if it does not exist.
  -e EMAILS --emails=EMAILS     Comma separated list of emails that will be
                                notified when the workflow finishes.
  -f --files                    List project files instead of jobs. The first
                                column is the local path of the file, the
                                second the path of the file in the archive.
  -h --help                     Show this message and exit.
  -i --include-properties       Include project properties with job options.
  -k --kill                     Kill worfklow on first job failure.
  -l --log                      Show path to current log file and exit.
  -o OPTIONS --options=OPTIONS  Comma separated list of options that will be
                                displayed next to each job. E.g. `-o type,foo`.
                                The resulting output will be tab separated.
  -p PROJECT --project=PROJECT  Azkaban project. Can either be a project name
                                or a path to a python module/package defining
                                an `azkaban.Project` instance. Commands which
                                are followed by an asterisk will only work in
                                the latter case.
  -r --replace                  Overwrite any existing file.
  -s --skip                     Skip if workflow is already running.
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
  temppath, write_properties)
from docopt import docopt
from requests.exceptions import HTTPError
from sys import stdout
import logging as lg
import os
import os.path as osp


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

def _parse__project(_project):
  """Parse `--project` argument into `(module, path, name)`.

  :param _project: `--project` argument.

  Note that `module` is guaranteed to be non `None`. `path` and `name` can.

  """
  _project = _project or Config().get_option('azkaban', 'project', 'jobs')
  if ':' in _project:
    location, name = _project.split(':', 1)
  else:
    location = _project
    name = None
  location = location.rstrip(os.sep)
  if os.sep in location:
    path, fpath = location.rsplit(os.sep, 1)
  else:
    path = None
    fpath = location
  module = osp.splitext(fpath)[0]
  return module, path, name

def _get_project_name(_project):
  """Return project name.

  :param _project: `--project` argument.

  This function will try to use the default path/module to infer a project name
  if none is provided.

  """
  module, path, name = _parse__project(_project)
  if name:
    return name
  else:
    return Project.load(module, path=path).name

def _load_project(_project):
  """Resolve project from CLI argument.

  :param _project: `--project` argument.

  """
  module, path, name = _parse__project(_project)
  try:
    return Project.load(module, path=path, name=name)
  except ImportError:
    if _project:
      msg = (
        'This command requires a project configuration module which was not '
        'found at `%s`.\nSpecify another project module location using the '
        '`--project` option.'
      ) % (_project, )
    else:
      msg = (
        'This command requires a project configuration module which was not '
        'found at the default\n`job` location. Specify another project module '
        'location using the `--project` option.'
      )
    raise AzkabanError(msg)

def _upload_callback(cur_bytes, tot_bytes, file_index):
  """Callback for streaming upload.

  :param cur_bytes: Total bytes uploaded so far.
  :param tot_bytes: Total bytes to be uploaded.
  :param file_index: (0-based) index of the file currently uploaded.

  """
  if cur_bytes != tot_bytes:
    stdout.write(
      'Uploading project: %.1f%%\r'
      % (100. * cur_bytes / tot_bytes, )
    )
  else:
    stdout.write('Validating project...    \r')
  stdout.flush()

def view_info(project, _files, _options, _job, _include_properties):
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
      stdout.write('%s\t%s\n' % (osp.relpath(path), archive_path))
  else:
    if _options:
      option_names = _options.split(',')
      for name, opts in sorted(project.jobs.items()):
        job_opts = '\t'.join(opts.get(o, '') for o in option_names)
        stdout.write('%s\t%s\n' % (name, job_opts))
    else:
      for name in sorted(project.jobs):
        stdout.write('%s\n' % (name, ))

def view_log(_execution, _job, _url, _alias):
  """View workflow or job execution logs."""
  session = Session(_url, _alias)
  exc = Execution(session, _execution)
  logs = exc.job_logs(_job[0]) if _job else exc.logs()
  try:
    for line in logs:
      stdout.write('%s\n' % (line.encode('utf-8'), ))
  except HTTPError:
    # Azkaban responds with 500 if the execution or job isn't found
    if _job:
      raise AzkabanError(
        'Execution %s and/or job %s not found.', _execution, _job
      )
    else:
      raise AzkabanError('Execution %s not found.', _execution)

def run_flow(project_name, _workflow, _job, _url, _alias, _skip, _kill,
  _emails):
  """Run workflow."""
  session = Session(_url, _alias)
  res = session.run_workflow(
    name=project_name,
    flow=_workflow,
    jobs=_job,
    concurrent=not _skip,
    on_failure='cancel' if _kill else 'finish',
    emails=_emails.split(',') if _emails else None,
  )
  exec_id = res['execid']
  job_names = ', jobs: %s' % (', '.join(_job), ) if _job else ''
  stdout.write(
    'Flow %s successfully submitted (execution id: %s%s).\n'
    'Details at %s/executor?execid=%s\n'
    % (_workflow, exec_id, job_names, session.url, exec_id)
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
  stdout.write(
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

def build_project(project, _zip, _url, _alias, _replace, _create):
  """Build project."""
  if _zip:
    if osp.isdir(_zip):
      _zip = osp.join(_zip, '%s.zip' % (project.versioned_name, ))
    project.build(_zip, overwrite=_replace)
    stdout.write(
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
      stdout.write(
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
def main():
  """Command line argument parser."""
  args = docopt(__doc__, version=__version__)
  # activate logging
  logger = lg.getLogger()
  logger.setLevel(lg.DEBUG)
  handler = Config().get_file_handler('azkaban')
  if handler:
    logger.addHandler(handler)
  # do things
  if args['--log']:
    if handler:
      stdout.write('%s\n' % (handler.baseFilename, ))
    else:
      raise AzkabanError('No log file active.')
  elif args['build']:
    build_project(
      _load_project(args['--project']),
      **_forward(args, ['ZIP', '--url', '--alias', '--replace', '--create'])
    )
  elif args['log']:
    view_log(
      **_forward(args, ['EXECUTION', 'JOB', '--url', '--alias'])
    )
  elif args['info']:
    view_info(
      _load_project(args['--project']),
      **_forward(args, ['--files', '--options', 'JOB', '--include-properties'])
    )
  elif args['run']:
    run_flow(
      _get_project_name(args['--project']),
      **_forward(
        args,
        ['WORKFLOW', 'JOB', '--skip', '--url', '--alias', '--kill', '--email']
      )
    )
  elif args['upload']:
    upload_project(
      _get_project_name(args['--project']),
      **_forward(args, ['ZIP', '--create', '--url', '--alias'])
    )

if __name__ == '__main__':
  main()
