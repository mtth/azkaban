#!/usr/bin/env python
# encoding: utf-8

"""Azkaban CLI: a lightweight command line interface for Azkaban.

Usage:
  azkaban run [-bp PROJECT] [-s SCRIPT] (-u URL | -a ALIAS) FLOW [JOB ...]
  azkaban upload [-cp PROJECT] [-s SCRIPT | -z ZIP] (-u URL | -a ALIAS)
  azkaban build [-op PROJECT] [-s SCRIPT] [-z ZIP]
  azkaban list [-p PROJECT] [-s SCRIPT] [-f | FLOW]
  azkaban view [-p PROJECT] [-s SCRIPT] JOB
  azkaban (create | delete) (-u URL | -a ALIAS)

  azkaban -h | --help | -v | --version

Commmands:
  create                        Create a project on Azkaban. Will be prompted
                                for a name and description.
  delete                        Delete a project on Azkaban. Will be prompted
                                for a name.
  run                           Run jobs or workflows. If no job is specified,
                                the entire workflow will be executed. The
                                workflow must have already been uploaded to the
                                server.
  upload                        Upload project to Azkaban server. If a script is
                                passed as argument, the project will be built to
                                a temporary file then uploaded. Note that the
                                project must have been created on the server
                                prior to uploading.
  build                         Build zip archive.
  list                          View list of jobs or other files inside a
                                project.
  view                          View job options.

Arguments:
  FLOW                          Workflow (job without children) name.
  JOB                           Job name.

Options:
  -a ALIAS --alias=ALIAS        Alias to saved URL and username. Will also try to
                                reuse session IDs for later connections.
  -b --block                    Don't run workflow concurrently if it is
                                already running.
  -c --create                   Create the project if it does not exist.
  -f --files                    List project files instead of jobs.
  -h --help                     Show this message and exit.
  -i --info                     Organize jobs by type and show dependencies. If
                                used with the `--files` option, will show the
                                size of each and its path in the archive.
  -o --overwrite                Overwrite any existing file.
  -p PROJECT --project=PROJECT  Azkaban project name. Necessary if more than one
                                project is defined in a configuration script.
  -s SCRIPT --script=SCRIPT     Project configuration script. This script must
                                contain an `azkaban.Project` instance with name
                                corresponding to PROJECT [default: jobs.py].
  -u URL --url=URL              Azkaban endpoint (with protocol, and optionally
                                a username): '[user@]protocol:endpoint'. E.g.
                                'http://azkaban.server'. The username defaults
                                to the current user, as determined by `whoami`.
                                If you often use the same url, consider using the
                                `--alias` option instead.
  -v --version                  Show version and exit.
  -z ZIP --zip=ZIP              For `upload` and `list` commands, the path to
                                an existing project zip archive. For `build`,
                                the path where the output archive will be
                                built (defaults to the project's name).

Examples:
  azkaban run -a my_alias my_flow
  azkaban upload -p my_project -u http://url.to.azkaban
  azkaban build -z archive.zip -s script.py

Azkaban CLI returns with exit code 1 if an error occurred and 0 otherwise.

"""


from azkaban import __version__
from azkaban.project import EmptyProject, Project
from azkaban.util import AzkabanError, human_readable, pretty_print, temppath
from collections import defaultdict
from docopt import docopt
from os import sep
from os.path import abspath, basename, dirname, getsize, relpath, splitext
from sys import exit, path, stdout, stderr


def main(project=None):
  """Command line argument parser.

  :param project: `EmptyProject` or `Project` instance

  """
  args = docopt(__doc__, version=__version__)
  name = args['--project']
  try:
    if args['run']:
      flow = args['FLOW']
      jobs = args['JOB']
      if not project:
        if name:
          project = EmptyProject(name)
        else:
          project = Project.load_from_script(args['--script'])
      session = project.get_session(url=args['--url'], alias=args['--alias'])
      res = project.run(
        flow=flow,
        url=session['url'],
        session_id=session['session_id'],
        jobs=jobs,
        block=args['--block'],
      )
      # TODO: make this change if only some jobs are submitted
      exec_id = res['execid']
      job_names = ', jobs: %s' % (', '.join(jobs), ) if jobs else ''
      stdout.write(
        'Flow %s successfully submitted (execution id: %s%s).\n'
        'Details at %s/executor?execid=%s\n'
        % (flow, exec_id, job_names, session['url'], exec_id)
      )
    elif args['create']:
      name = raw_input('Project name: ').strip()
      project = EmptyProject(name)
      session = project.get_session(url=args['--url'], alias=args['--alias'])
      description = raw_input('Project description [%s]: ' % (name, )) or name
      project.create(
        description=description,
        url=session['url'],
        session_id=session['session_id'],
      )
      stdout.write(
        'Project %s successfully created.\n'
        'Details at %s/manager?project=%s\n'
        % (project.name, session['url'], project.name)
      )
    elif args['delete']:
      name = raw_input('Project name: ')
      project = project or EmptyProject(name)
      session = project.get_session(url=args['--url'], alias=args['--alias'])
      project.delete(
        url=session['url'],
        session_id=session['session_id'],
      )
      stdout.write(
        'Project %s successfully deleted.\n'
        % (name, )
      )
    elif args['build']:
      project = project or Project.load_from_script(args['--script'], name)
      size = project.build(
        args['--zip'] or '%s.zip' % (project.name, ),
        overwrite=args['--overwrite'],
      )
      stdout.write(
        'Project successfully built (size: %s).\n'
        % (human_readable(size), )
      )
    elif args['upload']:
      path = args['--zip']
      if not project:
        if path:
          project = EmptyProject(name)
        else:
          project = Project.load_from_script(args['--script'], name)
      session = project.get_session(url=args['--url'], alias=args['--alias'])
      with temppath() as tpath:
        if not path:
          path = tpath
          size = project.build(path)
        else:
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
    elif args['list']:
      project = project or Project.load_from_script(args['--script'], name)
      if args['--info']:
        # TODO: change this to something less arbitrary
        if args['--files']:
          for path, apath in project._files.items():
            rpath = relpath(path)
            size = human_readable(getsize(path))
            apath = apath or abspath(path).lstrip(sep)
            stdout.write('%s: %s [%s]\n' % (rpath, size, apath))
        else:
          jobs = defaultdict(list)
          for name, job_options in project.jobs.items():
            job_type = job_options.get('type', '--')
            job_deps = job_options.get('dependencies', '')
            if job_deps:
              info = '%s [%s]' % (name, job_deps)
            else:
              info = name
            jobs[job_type].append(info)
          pretty_print(jobs)
      else:
        if args['--files']:
          for path in project._files:
            stdout.write('%s\n' % (relpath(path), ))
        else:
          for name in project.jobs:
            stdout.write('%s\n' % (name, ))
  except AzkabanError as err:
    stderr.write('%s\n' % (err, ))
    exit(1)

if __name__ == '__main__':
  main()
