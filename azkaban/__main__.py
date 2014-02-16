#!/usr/bin/env python
# encoding: utf-8

"""Azkaban CLI: a lightweight command line interface for Azkaban.

Usage:
  azkaban build [-fp PROJECT] [-a ALIAS | -u URL | ZIP]
  azkaban (create | delete) [-a ALIAS | -u URL]
  azkaban list [-p PROJECT] [-s | -o OPTIONS]
  azkaban run [-bp PROJECT]  [-a ALIAS | -u URL] FLOW [JOB ...]
  azkaban upload [-cp PROJECT] [-a ALIAS | -u URL] ZIP
  azkaban view [-p PROJECT] JOB
  azkaban -h | --help | -v | --version

Commmands:
  build*                        Build project. The resulting archive can either
                                be directly uploaded to Azkaban or saved
                                locally.
  create                        Create a project on Azkaban. Will be prompted
                                for a name and description.
  delete                        Delete a project on Azkaban. Will be prompted
                                for a name.
  list*                         View list of jobs or other files inside a
                                project.
  run                           Run jobs or workflows. If no job is specified,
                                the entire workflow will be executed. The
                                workflow must have already been uploaded to the
                                server.
  upload                        Upload archive to Azkaban server.
  view*                         View job options.

Arguments:
  FLOW                          Workflow (job without children) name.
  JOB                           Job name.
  ZIP                           For `upload` command, the path to an existing
                                project zip archive. For `build`, the path
                                where the output archive will be built.

Options:
  -a ALIAS --alias=ALIAS        Alias to saved URL and username. Will also try
                                to reuse session IDs for later connections.
  -b --block                    Don't run workflow concurrently if it is
                                already running.
  -c --create                   Create the project if it does not exist.
  -f --force                    Overwrite any existing file.
  -h --help                     Show this message and exit.
  -o OPTIONS --options=OPTIONS  Comma separated list of options that will be
                                displayed next to each job. E.g. `-o type,foo`.
  -p PROJECT --project=PROJECT  Azkaban project. Can either be a project name
                                or a path to file defining an `azkaban.Project`
                                instance. If more than one project is defined
                                in the script, you can disambiguate as follows:
                                `--project=jobs.py:my_project`. Some commands
                                (those followed by an asterisk) will only work
                                when passed a path to a configuration file.
  -s --static                   List static project files instead of jobs.
  -u URL --url=URL              Azkaban endpoint (with protocol, and optionally
                                a username): '[user@]protocol:endpoint'. E.g.
                                'http://azkaban.server'. The username defaults
                                to the current user, as determined by `whoami`.
                                If you often use the same url, consider using
                                the `--alias` option instead. Specifying a url
                                overrides any `--alias` option.
  -v --version                  Show version and exit.

Examples:
  azkaban run -a my_alias my_flow
  azkaban upload -p my_project -u http://url.to.azkaban
  azkaban build -z archive.zip -s script.py

Azkaban CLI returns with exit code 1 if an error occurred and 0 otherwise.

"""


from azkaban import __version__
from azkaban.project import Project
from azkaban.session import Session
from azkaban.util import AzkabanError, catch, human_readable, temppath
from docopt import docopt
from os.path import exists, getsize, relpath
from sys import stdout


def get_project(project, strict=False):
  """Resolve project from CLI argument.

  :param project: `--project` argument
  :param strict: only accept registered projects

  """
  parts = (project or '').rsplit(':', 1)
  if len(parts) == 1:
    if exists(parts[0]):
      project = Project.load_from_script(parts[0])
    elif not strict:
      project = Project(project, register=False)
    else:
      raise AzkabanError(
        'This command requires a registered project as `--project` option.\n'
        'Specify an existing project configuration script.'
      )
  else:
    project = Project.load_from_script(*parts)
  return project

@catch(AzkabanError)
def main():
  """Command line argument parser."""
  args = docopt(__doc__, version=__version__)
  if args['build']:
    project = get_project(args['--project'], strict=True)
    if args['ZIP']:
      path = args['ZIP']
      project.build(path, overwrite=args['--force'])
      stdout.write(
        'Project successfully built and saved as %r (size: %s).\n'
        % (path, human_readable(getsize(path)))
      )
    else:
      with temppath() as path:
        project.build(path)
        session = Session.from_url_or_alias(args['--url'], args['--alias'])
        res = session.upload_project(project, path)
        stdout.write(
          'Project %s successfully built and uploaded '
          '(id: %s, size: %s, version: %s).\n'
          'Details at %s/manager?project=%s\n'
          % (
            project,
            res['projectId'],
            human_readable(getsize(path)),
            res['version'],
            session.url,
            project,
          )
        )
  elif args['create']:
    session = Session.from_url_or_alias(args['--url'], args['--alias'])
    project = raw_input('Project name: ').strip()
    description = raw_input('Project description [%s]: ' % (project, ))
    session.create_project(project, description.strip() or project)
    stdout.write(
      'Project %s successfully created.\n'
      'Details at %s/manager?project=%s\n'
      % (project, session.url, project)
    )
  elif args['delete']:
    session = Session.from_url_or_alias(args['--url'], args['--alias'])
    project = raw_input('Project name: ')
    session.delete_project(project)
    stdout.write('Project %s successfully deleted.\n' % (project, ))
  elif args['list']:
    project = get_project(args['--project'], strict=True)
    if args['--static']:
      for path in project._files:
        stdout.write('%s\n' % (relpath(path), ))
    else:
      for name in project.jobs:
        stdout.write('%s\n' % (name, ))
  elif args['run']:
    project = get_project(args['--project'])
    flow = args['FLOW']
    jobs = args['JOB']
    session = Session.from_url_or_alias(args['--url'], args['--alias'])
    res = session.run_workflow(
      project=project,
      flow=flow,
      jobs=jobs,
      block=args['--block'],
    )
    exec_id = res['execid']
    job_names = ', jobs: %s' % (', '.join(jobs), ) if jobs else ''
    stdout.write(
      'Flow %s successfully submitted (execution id: %s%s).\n'
      'Details at %s/executor?execid=%s\n'
      % (flow, exec_id, job_names, session.url, exec_id)
    )
  elif args['upload']:
    path = args['ZIP']
    project = get_project(args['--project'])
    session = Session.from_url_or_alias(args['--url'], args['--alias'])
    while True:
      try:
        res = session.upload_project(project, path)
      except AzkabanError as err:
        if args['--create']:
          session.create_project(project, project)
        else:
          raise err
      else:
        break
    stdout.write(
      'Project %s successfully uploaded (id: %s, size: %s, version: %s).\n'
      'Details at %s/manager?project=%s\n'
      % (
        project,
        res['projectId'],
        human_readable(getsize(path)),
        res['version'],
        session.url,
        project,
      )
    )
  elif args['view']:
    project = get_project(args['--project'])
    job_name = args['JOB'][0]
    if job_name in project.jobs:
      for option, value in sorted(project.jobs[job_name].items()):
        stdout.write('%s=%s\n' % (option, value))
    else:
      raise AzkabanError('Job %r not found.' % (job_name, ))

if __name__ == '__main__':
  main()
