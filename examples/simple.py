#!/usr/bin/env python
# encoding: utf-8

"""Simple Azkaban project configuration script."""

from azkaban import Job, Project

project = Project('foo')
project.add_job('bar', Job({'type': 'command', 'command': 'echo "hi!"'}))
