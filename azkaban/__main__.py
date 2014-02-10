#!/usr/bin/env python
# encoding: utf-8

"""Azkaban CLI: a lightweight command line interface for Azkaban.

Usage:
  azkaban PROJECT run [-m MODE] (-u URL | -a ALIAS) FLOW [JOB ...]
  azkaban PROJECT upload [-s SCRIPT | -z ZIP] (-u URL | -a ALIAS)
  azkaban PROJECT build [-os SCRIPT] [-z ZIP]
  azkaban PROJECT list [-fps SCRIPT]
  azkaban PROJECT view [-s SCRIPT] JOB
  azkaban -h | --help | -v | --version

Commmands:
  run                         Run jobs or workflows. If no job is specified,
                              the entire workflow will be executed. The
                              workflow must have already been uploaded to the
                              server.
  upload                      Upload project to Azkaban server. If a script is
                              passed as argument, the project will be built to
                              a temporary file then uploaded. Note that the
                              project must have been created on the server
                              prior to uploading.
  build                       Build zip archive.
  list                        View list of jobs or other files inside a
                              project.
  view                        View job options.

Arguments:
  PROJECT                     Azkaban project name.
  FLOW                        Workflow (job without children) name.
  JOB                         Job name.

Options:
  -a ALIAS --alias=ALIAS      Alias to saved URL and username. Will also try to
                              reuse session IDs for later connections.
  -f --files                  List project files instead of jobs.
  -h --help                   Show this message and exit.
  -m MODE --mode=MODE         Run concurrency mode. Available options: skip
                              (do not run if this flow is already being
                              executed), concurrent (run flow concurrently).
                              pipeline1 (block jobs until their runs in previous
                              worklows have completed), pipeline2 (block jobs
                              until their children's runs have completed)
                              [default: concurrent].
  -o --overwrite              Overwrite any existing file.
  -p --pretty                 Organize jobs by type and show dependencies. If
                              used with the `--files` option, will show the
                              size of each and its path in the archive.
  -s SCRIPT --script=SCRIPT   Project configuration script. This script must
                              contain an `azkaban.Project` instance with name
                              corresponding to PROJECT [default: jobs.py].
  -u URL --url=URL            Azkaban endpoint (with protocol, and optionally
                              a username): '[user@]protocol:endpoint'. E.g.
                              'http://azkaban.server'. The username defaults
                              to the current user, as determined by `whoami`.
                              If you often use the same url, consider using the
                              `--alias` option instead.
  -v --version                Show version and exit.
  -z ZIP --zip=ZIP            For `upload` and `list` commands, the path to
                              an existing project zip archive. For `build`,
                              the path where the output archive will be
                              built (defaults to the project's name).

Examples:
  azkaban my_project run -a my_alias my_flow
  azkaban my_project upload -u http://url.to.azkaban
  azkaban my_project build -z archive.zip -s script.py

Azkaban CLI returns with exit code 1 if an error occurred and 0 otherwise.

"""


from azkaban import __version__
from azkaban.project import registry, EmptyProject
from azkaban.util import AzkabanError, pretty_print
from collections import defaultdict
from docopt import docopt
from os import sep
from os.path import basename, dirname, splitext
from sys import exit, path, stdout, stderr


def get_project(name, script):
  """Get project from script.

  :param name: project name
  :param script: string representing a python module containing a `Project`
    instance with a name corresponding to `name`

  """
  path.append(dirname(script))
  module = splitext(basename(script.rstrip(sep)))[0]
  try:
    __import__(module)
  except ImportError:
    raise AzkabanError('unable to import script %r' % (script, ))
  else:
    try:
      return registry[name]
    except KeyError:
      msg = 'unable to find project with name %r in script %r'
      raise AzkabanError(msg % (name, script))

def main(project=None):
  """Command line argument parser.

  :param project: `EmptyProject` or `Project` instance

  """
  args = docopt(__doc__, version=__version__)
  name = args['PROJECT']
  try:
    if args['run']:
      project = project or EmptyProject(name)
      project.run(
        flow=args['FLOW'],
        jobs=args['JOB'],
        url=args['--url'],
        alias=args['--alias'],
      )
    elif args['build']:
      project = project or get_project(name, args['--script'])
      project.build(
        args['--zip'] or '%s.zip' % (project.name, ),
        overwrite=args['--overwrite'],
      )
    elif args['upload']:
      if args['--zip']:
        project = project or EmptyProject(name)
        project.upload(
          args['--zip'],
          url=args['--url'],
          alias=args['--alias'],
        )
      else:
        project = project or get_project(name, args['--script'])
        with temppath() as path:
          project.build(path)
          project.upload(
            path,
            url=args['--url'],
            alias=args['--alias'],
          )
    elif args['view']:
      project = project or get_project(name, args['--script'])
      job_name = args['JOB'][0]
      if job_name in project.jobs:
        pretty_print(project.jobs[job_name])
      else:
        raise AzkabanError('missing job %r' % (job_name, ))
    elif args['list']:
      project = project or get_project(name, args['--script'])
      jobs = defaultdict(list)
      if args['--pretty']:
        if args['--files']:
          for path, apath in self._files.items():
            rpath = relpath(path)
            size = human_readable(getsize(path))
            apath = apath or abspath(path).lstrip(sep)
            stdout.write('%s: %s [%s]\n' % (rpath, size, apath))
        else:
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
          for path in self._files:
            stdout.write('%s\n' % (relpath(path), ))
        else:
          for name in project.jobs:
            stdout.write('%s\n' % (name, ))
  except AzkabanError as err:
    stderr.write('%s\n' % (err, ))
    exit(1)

if __name__ == '__main__':
  main()
