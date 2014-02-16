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
from azkaban.util import AzkabanError, Session, human_readable, temppath
from docopt import docopt
from os.path import getsize, relpath
from sys import exit, stdout, stderr


def resolve(project):
  """Resolve project from CLI argument.

  :param project: `--project` argument

  """
  parts = project.rsplit(':', 1)
  return Project.load_from_script(*parts)

def get_session(url=None, alias=None):
  """Get session from CLI options.

  :param url: `--url` option
  :param alias: `--alias` option

  """
  if url:
    session = Session(url)
  else:
    alias = alias or config.get_default_option('azkaban', 'alias')
    session = Session(**config.resolve_alias(alias))
  return session

def main():
  """Command line argument parser."""
  args = docopt(__doc__, version=__version__)
  config = Config()
  try:
    if args['build']:
      project = resolve(args['--project'])
      if args['--zip']:
        path = args['--zip']
        project.build(path, overwrite=args['--overwrite'])
        stdout.write(
          'Project successfully built and saved as %r (size: %s).\n'
          % (path, human_readable(size))
        )
      else:
        with temppath() as path:
          project.build(path)
          session = get_session(url=args['--url'], alias=args['--alias'])
          res = session.upload_project(project, path)
          stdout.write(
            'Project %s successfully built and uploaded '
            '(id: %s, size: %s, version: %s).\n'
            'Details at %s/manager?project=%s\n'
            % (
              project,
              res['projectId'],
              human_readable(get_size(path)),
              res['version'],
              session.url,
              project,
            )
    elif args['create']:
      session = get_session(url=args['--url'], alias=args['--alias'])
      project = raw_input('Project name: ').strip()
      description = raw_input('Project description [%s]: ' % (name, )) or name
      session.create_project(project, description)
      stdout.write(
        'Project %s successfully created.\n'
        'Details at %s/manager?project=%s\n'
        % (project, session.url, project)
      )
    elif args['delete']:
      session = get_session(url=args['--url'], alias=args['--alias'])
      project = raw_input('Project name: ')
      session.delete_project(project)
      stdout.write('Project %s successfully deleted.\n' % (name, ))
    elif args['list']:
      project = resolve(args['--project'])
      if args['--files']:
        for path in project._files:
          stdout.write('%s\n' % (relpath(path), ))
      else:
        for name in project.jobs:
          stdout.write('%s\n' % (name, ))
    elif args['run']:
      flow = args['FLOW']
      jobs = args['JOB']
      if not project:
        if name:
          project = EmptyProject(name)
        else:
          project = Project.load_from_script(args['--script'])
      session = Session(url=args['--url'], alias=args['--alias'])
      res = project.run(
        session=session,
        flow=flow,
        jobs=jobs,
        block=args['--block'],
      )
      exec_id = res['execid']
      job_names = ', jobs: %s' % (', '.join(jobs), ) if jobs else ''
      stdout.write(
        'Flow %s successfully submitted (execution id: %s%s).\n'
        'Details at %s/executor?execid=%s\n'
        % (flow, exec_id, job_names, session['url'], exec_id)
      )
    elif args['upload']:
      path = args['--zip']
      if not project:
        if path:
          if name:
            project = EmptyProject(name)
          else:
            raise AzkabanError('Unspecified project name. Use -p option.')
        else:
          project = Project.load_from_script(args['--script'], name)
      session = get_session(url=args['--url'], alias=args['--alias'])
      with temppath() as tpath:
        if not path:
          path = tpath
          project.build(path)
        size = getsize(path)
        try:
          res = project.upload(
            path,
            url=session['url'],
            session_id=session['session_id'],
          )
        except AzkabanError as err:
          if args['--create']:
            project.create(
              description=project.name,
              url=session['url'],
              session_id=session['session_id'],
            )
            res = project.upload(
              path,
              url=session['url'],
              session_id=session['session_id'],
            )
          else:
            raise err
      stdout.write(
        'Project %s successfully uploaded (id: %s, size: %s, version: %s).\n'
        'Details at %s/manager?project=%s\n'
        % (
          project.name,
          res['projectId'],
          human_readable(size),
          res['version'],
          session['url'],
          project.name
        )
      )
    elif args['view']:
      project = project or Project.load_from_script(args['--script'], name)
      job_name = args['JOB'][0]
      if job_name in project.jobs:
        pretty_print(project.jobs[job_name])
      else:
        raise AzkabanError('missing job %r' % (job_name, ))
  except AzkabanError as err:
    stderr.write('%s\n' % (err, ))
    exit(1)

if __name__ == '__main__':
  main()
