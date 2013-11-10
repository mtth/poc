#!/usr/bin/env python
# encoding: utf-8

"""Poc: Pig documentation tool.

Usage:
  poc PATH
  poc -h | --help | -v | --version

Arguments:
  PATH              Path to pig script

Options:
  -h --help         Print this message and exit.
  -v --version      Print version and exit.

"""

# before pip
# --------------------

from os import getcwd
from sys import path

path.append(getcwd())

# --------------------


from docopt import docopt
from poc import __version__
from poc.flow import Flow


def main(args):
  """TODO: main docstring.

  :param args: TODO

  """
  for operator in Flow(args['PATH']):
    print operator


if __name__ == '__main__':
  main(docopt(__doc__, version=__version__))
