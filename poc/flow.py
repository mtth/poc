#!/usr/bin/env python
# encoding: utf-8

"""Poc: Pig doc.

TODO:

* implement better operator line attribute
* allow comments (inline and standalone)


"""


from .util import PocError
from tokenize import generate_tokens

import logging


LOGGER = logging.getLogger(__name__)


class State(object):

  """Base class to hold parser state.

  :param state: previous state.

  """

  def __init__(self, state=None):
    self._buffer = []
    self.inputs = state.inputs if state else set()
    self.operator = state.operator if state else ''
    self.outputs = state.outputs if state else set()

  def __repr__(self):
    return '<%s (operator=%r, inputs=%r, outputs=%r)>' % (
      self.__class__.__name__, self.operator, self.inputs, self.outputs,
    )

  def transition(self, token):
    """Method called on each token.

    :param token: string

    To be overriden in subclasses.

    """
    raise NotImplementedError()


class InitialState(State):

  def transition(self, token):
    if not self._buffer:
      self._buffer.append(token)
      return self
    elif token == '=':
      self.outputs = frozenset([self._buffer[0]])
      return OperatorState(self)
    else:
      self.operator = self._buffer[0].upper()
      if self.operator == 'REGISTER':
        return WaitState(self)
      else:
        self.inputs = frozenset([token])
        if self.operator == 'SPLIT':
          return SplitState(self)
        else:
          return WaitState(self)


class WaitState(State):

  def transition(self, token):
    return self


class OperatorState(State):

  def transition(self, token):
    self.operator = token.upper()
    if self.operator == 'LOAD':
      return WaitState(self)
    elif self.operator in ['CROSS', 'STREAM', 'GROUP', 'JOIN']:
      return MultipleInputState(self)
    elif self.operator == 'MAPREDUCE':
      raise PocError('mapreduce operator not implemented yet')
    else:
      return SingleInputState(self)


class SplitState(State):

  def transition(self, token):
    if token.upper() in ['IF', 'OTHERWISE']:
      self.outputs.add(self._buffer[-1])
    else:
      self._buffer.append(token)
    return self


class SingleInputState(State):

  def transition(self, token):
    self.inputs = frozenset([token])
    return WaitState(self)


class MultipleInputState(State):

  level = 0
  pending = True

  def transition(self, token):
    if token in ['(', '{']:
      self.level += 1
    elif token in [')', '}']:
      self.level -= 1
    elif not self.level:
      if self.pending:
        self.inputs.add(token)
        self.pending = False
      elif token == ',':
        self.pending = True
      else:
        return WaitState(self)
    return self


class Operator(object):

  """Operator.

  :param name: operator name
  :param line: line where operator is defined
  :param parents: dictionary of {alias: operator} parents
  :param flow: main flow

  """

  def __init__(self, name, line, parents, flow):
    self.name = name
    self.line = line
    self.parents = parents
    self.flow = flow
    LOGGER.debug('created %r', self)

  def __repr__(self):
    return '<%s>' % (self.name, )

  def __str__(self):
    return self.line

  @property
  def children(self):
    """Children operators."""
    return {
      name: operator
      for operator in self.flow
      for name, parent_operator in operator.parents.items()
      if self == parent_operator
    }


class Flow(object):

  """Flow.

  :param path: path to pig script

  Usage::

    for operator in Flow('a.pig'):
      print operator.line

  """

  def __init__(self, path):
    self._available_inputs = {}
    self.operators = list(self._generate_operators(path))

  def __iter__(self):
    return iter(self.operators)

  def _generate_operators(self, path):
    """Operator generator.

    :param path: path to pig script.

    """
    state = InitialState()
    with open(path) as reader:
      generator = (
        (e[1], e[4])
        for e in generate_tokens(reader.readline)
      )
      for token, line in generator:
        LOGGER.debug('parsing token %r', token)
        if token == ';' and state.operator:
          try:
            parents = {
              name: self._available_inputs[name]
              for name in state.inputs
            }
          except KeyError as err:
            raise PocError('undefined alias %s' % (err, ))
          operator = Operator(
            name=state.operator,
            parents=parents,
            line=line,
            flow=self,
          )
          for name in state.outputs:
            self._available_inputs[name] = operator
          yield operator
          state = InitialState()
        elif token != '\n':
          state = state.transition(token)
