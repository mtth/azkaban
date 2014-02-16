#!/usr/bin/env python
# encoding: utf-8

"""Azkaban CLI: a lightweight command line interface for Azkaban.

Usage:
  azkaban build [-rp PROJECT] [-a ALIAS | -u URL | ZIP]
  azkaban (create | delete) [-a ALIAS | -u URL]
  azkaban info [-p PROJECT] [-f | -o OPTIONS | JOB]
  azkaban run [-sp PROJECT]  [-a ALIAS | -u URL] FLOW [JOB ...]
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
  -f --files                    List project files instead of jobs.
  -h --help                     Show this message and exit.
  -o OPTIONS --options=OPTIONS  Comma separated list of options that will be
                                displayed next to each job. E.g. `-o type,foo`.
                                The resulting output will be tab separated.
  -p PROJECT --project=PROJECT  Azkaban project. Can either be a project name
                                or a path to file defining an `azkaban.Project`
                                instance. If more than one project is defined
                                in the script, you can disambiguate as follows:
                                `--project=jobs.py:my_project`. Commands which
                                are followed by an asterisk will only work when
                                passed a path to a configuration file.
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
from azkaban.session import Session
from azkaban.util import AzkabanError, catch, human_readable, temppath
from docopt import docopt
from os.path import exists, getsize
from sys import stdout


def _forward(args, names):
  """Forward subset of arguments from initial dictionary.

  :param args: dictionary of parsed arguments (output of `docopt.docopt`)
  :param names: list of names that will be included

  """
  names = set(names)
  return dict(
    (k.lower().lstrip('-'), v)
    for (k, v) in args.items() if k in names
  )

def _get_project_name(project_arg):
  """Return project name.

  :param project_arg: `--project` argument

  """
  parts = (project_arg or 'jobs.py').rsplit(':', 1)
  if len(parts) == 1:
    if exists(parts[0]):
      return Project.load_from_script(parts[0]).name
    else:
      return parts[0]
  else:
    return parts[1]

def _load_project(project_arg):
  """Resolve project from CLI argument.

  :param project_arg: `--project` argument

  """
  parts = (project_arg or 'jobs.py').rsplit(':', 1)
  return Project.load_from_script(*parts)

def build_project(project, zip, url, alias, replace):
  """Build project.

  :param args: dictionary of parsed arguments (output of `docopt.docopt`)

  Argument name forces `zip` redefinition here. Oh well.

  """
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
      res = session.upload_project(project, zip)
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
  """Create new project on remote Azkaban server.

  :param args: dictionary of parsed arguments (output of `docopt.docopt`)

  """
  session = Session(url, alias)
  project = raw_input('Project name: ').strip()
  description = raw_input('Project description [%s]: ' % (project, ))
  session.create_project(project, description.strip() or project)
  stdout.write(
    'Project %s successfully created.\n'
    'Details at %s/manager?project=%s\n'
    % (project, session.url, project)
  )

def delete_project(url, alias):
  """Delete a project on remote Azkaban server.

  :param args: dictionary of parsed arguments (output of `docopt.docopt`)

  """
  session = Session(url, alias)
  project = raw_input('Project name: ')
  session.delete_project(project)
  stdout.write('Project %s successfully deleted.\n' % (project, ))

def view_info(project, files, options, job):
  """List jobs in project.

  :param args: dictionary of parsed arguments (output of `docopt.docopt`)

  """
  if job:
    job_name = job[0]
    if job_name in project.jobs:
      for option, value in sorted(project.jobs[job_name].items()):
        stdout.write('%s=%s\n' % (option, value))
    else:
      raise AzkabanError('Job %r not found.' % (job_name, ))
  elif files:
    for path in project.files:
      stdout.write('%s\n' % (path, ))
  else:
    if options:
      option_names = options.split(',')
      for name, opts in project.jobs.items():
        job_opts = '\t'.join(opts.get(o, '') for o in option_names)
        stdout.write('%s\t%s\n' % (name, job_opts))
    else:
      for name in project.jobs:
        stdout.write('%s\n' % (name, ))

def run_flow(project_name, flow, job, url, alias, skip):
  """Run workflow.

  :param args: dictionary of parsed arguments (output of `docopt.docopt`)

  """
  session = Session(url, alias)
  res = session.run_workflow(project_name, flow, job, skip)
  exec_id = res['execid']
  job_names = ', jobs: %s' % (', '.join(job), ) if job else ''
  stdout.write(
    'Flow %s successfully submitted (execution id: %s%s).\n'
    'Details at %s/executor?execid=%s\n'
    % (flow, exec_id, job_names, session.url, exec_id)
  )

def upload_project(project_name, zip, url, alias, create):
  """Upload project.

  :param args: dictionary of parsed arguments (output of `docopt.docopt`)

  """
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
      **_forward(args, ['ZIP', '--url', '--alias', '--replace'])
    )
  elif args['create']:
    create_project(**_forward(args, ['--url', '--alias']))
  elif args['delete']:
    delete_project(**_forward(args, ['--url', '--alias']))
  elif args['info']:
    view_info(
      _load_project(args['--project']),
    **_forward(args, ['--files', '--options', 'JOB']))
  elif args['run']:
    run_flow(
      _get_project_name(args['--project']),
      **_forward(args, ['FLOW', 'JOB', '--skip', '--url', '--alias'])
    )
  elif args['upload']:
    upload_project(
      _get_project_name(args['--project']),
      **_forward(args, ['ZIP', '--create', '--url', '--alias'])
    )

if __name__ == '__main__':
  main()
