#!/usr/bin/env python
# encoding: utf-8

"""Azkaban CLI: a lightweight command line interface for Azkaban.

Usage:
  azkaban PROJECT run [-m MODE] (-u URL | -a ALIAS) FLOW [JOB ...]
  azkaban PROJECT upload (-u URL | -a ALIAS) [SCRIPT | -z ZIP]
  azkaban PROJECT build [-oz ZIP] [SCRIPT]
  azkaban PROJECT list [-fp] [SCRIPT]
  azkaban PROJECT view JOB [SCRIPT]
  azkaban -h | --help | -v | --version

Commmands:
  run                           Run jobs or workflows. If no job is specified,
                                the entire workflow will be executed. The
                                workflow must have already been uploaded to the
                                server.
  upload                        Upload project to Azkaban server. If a script
                                is passed as argument, the project will be
                                built to a temporary file then uploaded. Note
                                that the project must have been created on the
                                server prior to uploading.
  build                         Build zip archive.
  list                          View list of jobs or other files inside a
                                project.
  view                          View job options.

Arguments:
  PROJECT                       Azkaban project name.
  SCRIPT                        Project configuration script. This script must
                                contain an `azkaban.Project` instance with name
                                corresponding to PROJECT. If not specified,
                                Azkaban CLI will look for a file named
                                'jobs.py' in the current working directory.
  FLOW                          Workflow (job without children) name.
  JOB                           Job name.

Options:
  -a ALIAS --alias=ALIAS        Alias to saved URL and username. Will also try
                                to reuse session IDs for later connections.
  -f --files                    List project files instead of jobs.
  -h --help                     Show this message and exit.
  -m MODE --mode=MODE           Run concurrency mode. Available options: skip
                                (do not run if this flow is already being
                                executed), ... [default: ...].
  -o --overwrite                Overwrite any existing file.
  -p --pretty                   Organize jobs by type and show dependencies. If
                                used with the `--files` option, will show the
                                size of each and its path in the archive.
  -u URL --url=URL              Azkaban endpoint (with protocol), for example:
                                'http://azkaban.server'. Optionally, you can
                                specify a username: 'me@http://azkaban.server'
                                (defaults to the current user, as determined by
                                `whoami`). If you often use the same url,
                                consider using the `--alias` option instead.
  -v --version                  Show version and exit.
  -z ZIP --zip=ZIP              For `upload` and `list` commands, the path to
                                an existing project zip archive. For `build`,
                                the path where the output archive will be
                                built (defaults to the project's name).

Examples:
  azkaban my_project run -a my_alias my_flow
  azkaban my_project upload -u http://url.to.azkaban
  azkaban my_project build -z archive.zip script.py

Azkaban CLI returns with exit code 1 if an error occurred and 0 otherwise.

"""


from azkaban import __version__
from azkaban.project import registry, EmptyProject
from azkaban.util import AzkabanError
from docopt import docopt
from os import sep
from os.path import basename, dirname, splitext
from sys import exit, path, stdout, stderr


def get_project(name, script=None):
  """Get project from script.

  :param name: project name
  :param script: python module containing a `Project` instance with a
    corresponding name

  """
  script = script or 'jobs.py'
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
  print args
  try:
    if not project:
      script = args['SCRIPT']
      if script:
        project = get_project(name, script)
      else:
        project = EmptyProject(name)
    if args['run']:
      self.run(
        flow=args['FLOW'],
        url=args['URL'],
        user=args['--user'],
        alias=args['--alias'],
      )
    if args['build']:
      project.build(
        args['--zip'] or '%s.zip' % (project.name, ),
        overwrite=args['--overwrite']
      )
    elif args['upload']:
      if args['--zip']:
        self.upload(
          args['--zip'],
          url=args['URL'],
          user=args['--user'],
          alias=args['--alias'],
        )
      else:
        with temppath() as path:
          self.build(path)
          self.upload(
            path,
            url=args['URL'],
            user=args['--user'],
            alias=args['--alias'],
          )
    elif args['view']:
      job_name = args['JOB']
      if job_name in self._jobs:
        job = self._jobs[job_name]
        pretty_print(job.build_options)
      else:
        raise AzkabanError('missing job %r' % (job_name, ))
    elif args['list']:
      jobs = defaultdict(list)
      if args['--pretty']:
        if args['--files']:
          for path, apath in self._files.items():
            rpath = relpath(path)
            size = human_readable(getsize(path))
            apath = apath or abspath(path).lstrip(sep)
            stdout.write('%s: %s [%s]\n' % (rpath, size, apath))
        else:
          for name, job in self._jobs.items():
            job_type = job.build_options.get('type', '--')
            job_deps = job.build_options.get('dependencies', '')
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
          for name in self._jobs:
            stdout.write('%s\n' % (name, ))
  except AzkabanError as err:
    stderr.write('%s\n' % (err, ))
    exit(1)

if __name__ == '__main__':
  main()
