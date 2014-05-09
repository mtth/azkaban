#!/usr/bin/env python
# encoding: utf-8

"""Azkaban CLI: a lightweight command line interface for Azkaban.

Usage:
  azkaban build [-crp PROJECT] [-a ALIAS | -u URL | ZIP]
  azkaban create [-a ALIAS | -u URL]
  azkaban delete [-a ALIAS | -u URL]
  azkaban info [-p PROJECT] [-f | -o OPTIONS | [-i] JOB ...]
  azkaban run [-ksp PROJECT] [-a ALIAS | -u URL] [-e EMAIL] FLOW [JOB ...]
  azkaban upload [-cp PROJECT] [-a ALIAS | -u URL] ZIP
  azkaban -h | --help | -v | --version

Commmands:
  build*                        Build project and upload to Azkaban or save
                                locally the resulting archive.
  create                        Create a project on the Azkaban server.
  delete                        Delete a project on the Azkaban server.
  info*                         View information about jobs or files.
  run                           Run jobs or workflows. If no job is specified,
                                the entire workflow will be executed.
  upload                        Upload archive to Azkaban server.

Arguments:
  FLOW                          Workflow (job without children) name.
  JOB                           Job name.
  ZIP                           For `upload` command, the path to an existing
                                project zip archive. For `build`, the path
                                where the output archive will be built.

Options:
  -a ALIAS --alias=ALIAS        Alias to saved URL and username. Will also try
                                to reuse session IDs for later connections.
  -c --create                   Create the project if it does not exist.
  -e EMAIL --email=EMAIL        Comma separated list of emails that will be
                                notified when the workflow finishes.
  -f --files                    List project files instead of jobs. The first
                                column is the local path of the file, the
                                second the path of the file in the archive.
  -h --help                     Show this message and exit.
  -i --include-properties       Include project properties with job options.
  -k --kill                     Kill worfklow on first job failure.
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
from azkaban.remote import Session
from azkaban.util import (AzkabanError, Config, catch, human_readable,
  temppath, write_properties)
from docopt import docopt
from os.path import exists, getsize, relpath
from sys import stdout


def _forward(args, names):
  """Forward subset of arguments from initial dictionary.

  :param args: Dictionary of parsed arguments (output of `docopt.docopt`).
  :param names: List of names that will be included.

  """
  names = set(names)
  return dict(
    (k.lower().lstrip('-').replace('-', '_'), v)
    for (k, v) in args.items() if k in names
  )

def _get_project_name(project_arg):
  """Return project name.

  :param project_arg: `--project` argument.

  """
  if not project_arg:
    project_arg = Config().get_option('azkaban', 'project', 'jobs.py')
  parts = project_arg.split(':', 1)
  if len(parts) == 1:
    if exists(parts[0]):
      return Project.load(parts[0]).name
    else:
      return parts[0]
  else:
    return Project.load(*parts).name

def _load_project(project_arg):
  """Resolve project from CLI argument.

  :param project_arg: `--project` argument.

  """
  default_script = Config().get_option('azkaban', 'project', 'jobs.py')
  if not project_arg:
    script = default_script
    name = None
  elif ':' in project_arg:
    script, name = project_arg.split(':', 1)
  elif exists(project_arg):
    script = project_arg
    name = None
  else:
    script = default_script
    name = project_arg
  if not exists(script):
    raise AzkabanError(
      'This command requires a project configuration file which was not found '
      'at\n%slocation %r. Specify another path using the `--project` option.'
      % ('default ' if script == default_script else '', script)
    )
  return Project.load(script, name)

def build_project(project, zip, url, alias, replace, create):
  """Build project."""
  if zip:
    project.build(zip, overwrite=replace)
    stdout.write(
      'Project successfully built and saved as %r (size: %s).\n'
      % (zip, human_readable(getsize(zip)))
    )
  else:
    with temppath() as zip:
      project.build(zip)
      session = Session(url, alias)
      while True:
        try:
          res = session.upload_project(project.name, zip)
        except AzkabanError as err:
          if create and str(err).endswith("doesn't exist."):
            session.create_project(project.name, project.name)
          else:
            raise err
        else:
          break
      stdout.write(
        'Project %s successfully built and uploaded '
        '(id: %s, size: %s, version: %s).\n'
        'Details at %s/manager?project=%s\n'
        % (
          project,
          res['projectId'],
          human_readable(getsize(zip)),
          res['version'],
          session.url,
          project,
        )
      )

def create_project(url, alias):
  """Create new project on remote Azkaban server."""
  session = Session(url, alias)
  name = raw_input('Project name: ').strip()
  description = raw_input('Project description [%s]: ' % (name, ))
  session.create_project(name, description.strip() or name)
  stdout.write(
    'Project %s successfully created.\n'
    'Details at %s/manager?project=%s\n'
    % (name, session.url, name)
  )

def delete_project(url, alias):
  """Delete a project on remote Azkaban server."""
  session = Session(url, alias)
  project = raw_input('Project name: ')
  session.delete_project(project)
  stdout.write('Project %s successfully deleted.\n' % (project, ))

def view_info(project, files, options, job, include_properties):
  """List jobs in project."""
  if job:
    if include_properties:
      write_properties(project.properties, header='project.properties')
    for name in job:
      project.jobs[name].build(header='%s.job' % (name, ))
  elif files:
    for path, archive_path in sorted(project.files):
      stdout.write('%s\t%s\n' % (relpath(path), archive_path))
  else:
    if options:
      option_names = options.split(',')
      for name, opts in sorted(project.jobs.items()):
        job_opts = '\t'.join(opts.get(o, '') for o in option_names)
        stdout.write('%s\t%s\n' % (name, job_opts))
    else:
      for name in sorted(project.jobs):
        stdout.write('%s\n' % (name, ))

def run_flow(project_name, flow, job, url, alias, skip, kill, email):
  """Run workflow."""
  session = Session(url, alias)
  res = session.run_workflow(
    name=project_name,
    flow=flow,
    jobs=job,
    concurrent=not skip,
    on_failure='cancel' if kill else 'finish',
    emails=email.split(',') if email else None,
  )
  exec_id = res['execid']
  job_names = ', jobs: %s' % (', '.join(job), ) if job else ''
  stdout.write(
    'Flow %s successfully submitted (execution id: %s%s).\n'
    'Details at %s/executor?execid=%s\n'
    % (flow, exec_id, job_names, session.url, exec_id)
  )

def upload_project(project_name, zip, url, alias, create):
  """Upload project."""
  session = Session(url, alias)
  while True:
    try:
      res = session.upload_project(project_name, zip)
    except AzkabanError as err:
      if create:
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
      human_readable(getsize(zip)),
      res['version'],
      session.url,
      project_name,
    )
  )

@catch(AzkabanError)
def main():
  """Command line argument parser."""
  args = docopt(__doc__, version=__version__)
  if args['build']:
    build_project(
      _load_project(args['--project']),
      **_forward(args, ['ZIP', '--url', '--alias', '--replace', '--create'])
    )
  elif args['create']:
    create_project(**_forward(args, ['--url', '--alias']))
  elif args['delete']:
    delete_project(**_forward(args, ['--url', '--alias']))
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
        ['FLOW', 'JOB', '--skip', '--url', '--alias', '--kill', '--email']
      )
    )
  elif args['upload']:
    upload_project(
      _get_project_name(args['--project']),
      **_forward(args, ['ZIP', '--create', '--url', '--alias'])
    )

if __name__ == '__main__':
  main()
