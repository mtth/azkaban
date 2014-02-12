#!/usr/bin/env python
# encoding: utf-8

"""AzkabanPig: an extension for pig scripts to Azkaban CLI.

Usage:
  azkabanpig [-p PROJECT] [-t TYPE] (-u URL | -a ALIAS) PATH ...
             [-j JAR] ... [-o OPTION] ...
  azkabanpig -h | --help

Arguments:
  PATH                          Path to pig script. If more than one path is
                                specified, they will be run in order. Note that
                                all the pig jobs will share the same Azkaban
                                options; for more flexibility, you can use the
                                Azkaban CLI directly.

Options:
  -a ALIAS --alias=ALIAS        Cf. `azkaban --help`.
  -h --help                     Show this message and exit.
  -j JAR --jar=JAR              Path to jar file. It will be available on the
                                class path when the pig script is run, no need
                                to register it inside your scripts.
  -o OPTION --option=OPTION     Azkaban option. Should be of the form
                                key=value. E.g. '-o param.foo=bar' will
                                substitute parameter '$foo' with 'bar' in the
                                pig script.
  -p PROJECT --project=PROJECT  Project name under which to run the pig script
                                [default: pig_${user}].
  -t TYPE --type=TYPE           Pig job type used [default: pig].
  -u URL --url=URL              Cf. `azkaban --help`.

Examples:
  azkabanpig -a my_alias my_script.pig
  azkabanpig -url http://url.to.azkaban -o param.my_output=bar.dat foo.pig

AzkabanPig returns with exit code 1 if an error occurred and 0 otherwise.

"""

from docopt import docopt
from getpass import getuser
from os import sep
from os.path import abspath, basename
from string import Template
from sys import exit, stderr, stdout
from ..job import PigJob
from ..project import Project
from ..util import AzkabanError, temppath


class PigProject(Project):

  """Project to run pig jobs from the command line.

  :param name: project name used
  :param paths: paths to pig scripts, these will be run in order
  :param pig_type: pig job type used
  :param jars: jars to include, these will also be added to the classpath
  :param options: options forwarded to pig scripts

  """

  def __init__(self, name, paths, pig_type='pig', jars=None, options=None):
    super(PigProject, self).__init__(name, register=False)
    jars = jars or []
    opts = [
      {
        'type': pig_type,
        'user.to.proxy': getuser(),
        'pig.additional.jars': ','.join(abspath(j).lstrip(sep) for j in jars),
      },
      options or {}
    ]
    for jar in jars:
      self.add_file(abspath(jar))
    for path, dep in zip(paths, [None] + paths):
      dep_opts = {'dependencies': basename(dep)} if dep else {}
      self.add_job(basename(path), PigJob(abspath(path), dep_opts, *opts))


def main():
  """AzkabanPig entry point."""
  args = docopt(__doc__)
  project_name = Template(args['--project']).substitute(user=getuser())
  paths = args['PATH']
  try:
    try:
      job_options = dict(opt.split('=', 1) for opt in args['--option'])
    except ValueError:
      raise AzkabanError('Invalid options: %r' % (' '.join(args['OPTION']), ))
    project = PigProject(
      name=project_name,
      paths=paths,
      pig_type=args['--type'],
      jars=args['--jar'],
      options=job_options,
    )
    session = project.get_session(url=args['--url'], alias=args['--alias'])
    url = session['url']
    session_id = session['session_id']
    with temppath() as tpath:
      project.build(tpath)
      try:
        project.upload(tpath, url, session_id)
      except AzkabanError as err:
        try:
          project.create(project.name, url, session_id)
        except AzkabanError:
          raise err
        else:
          project.upload(tpath, url, session_id)
    res = project.run(basename(paths[-1]), url, session_id)
    exec_id = res['execid']
    if len(paths) == 1:
      stdout.write(
        'Pig job running at %s/executor?execid=%s&job=%s\n'
        % (session['url'], exec_id, basename(paths[0]))
      )
    else:
      stdout.write(
        'Pig jobs workflow running at %s/executor?execid=%s\n'
        % (session['url'], exec_id)
      )
  except AzkabanError as err:
    stderr.write('%s\n' % (err, ))
    exit(1)

if __name__ == '__main__':
  main()
