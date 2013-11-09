#!/usr/bin/env python
# encoding: utf-8

"""Poc: Pig doc.

No inline comments allowed currently.

"""


from .util import PocError
from tokenize import generate_tokens

import logging


LOGGER = logging.getLogger(__name__)


class State(object):

  """Current state.

  :param state: previous state

  """

  def __init__(self, state=None):
    self.operator = state.operator if state else ''
    self.inputs = state.inputs if state else set()
    self.outputs = state.outputs if state else set()
    self._buffer = []

  def __repr__(self):
    return '<%s (operator=%r, inputs=%r, outputs=%r)>' % (
      self.__class__.__name__, self.operator, self.inputs, self.outputs,
    )

  def transition(self, token, line, callback):
    """Return next state.

    :param token: TODO
    :param line: TODO
    :param callback: TODO

    TODO: fix line

    """
    if token == ';':
      callback(
        operator=self.operator,
        inputs=self.inputs,
        outputs=self.outputs,
        line=line,
      )
      new_state = InitialState()
    elif token == '\n':
      new_state = self
    else:
      new_state = self._transition(token)
    LOGGER.debug('%r -> %r', token, new_state)
    return new_state

  def _transition(self, token):
    """Method called on each token.

    :param token: string

    To be overriden in subclasses.

    """
    raise NotImplementedError()


class InitialState(State):

  def _transition(self, token):
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

  def _transition(self, token):
    return self


class OperatorState(State):

  def _transition(self, token):
    self.operator = token.upper()
    if self.operator == 'GROUP':
      return GroupState(self)
    if self.operator == 'JOIN':
      return JoinState(self)
    elif self.operator == 'LOAD':
      return WaitState(self)
    elif self.operator in ['CROSS', 'STREAM']:
      return MultipleInputState(self)
    else:
      return SingleInputState(self)


class SplitState(State):

  def _transition(self, token):
    if token.upper() in ['IF', 'OTHERWISE']:
      self.outputs.add(self._buffer[-1])
    else:
      self._buffer.append(token)
    return self


class KeywordState(State):

  keywords = frozenset()
  endwords = frozenset()

  def _transition(self, token):
    token_upper = token.upper()
    if token_upper in self.endwords:
      return WaitState(self)
    else:
      if token_upper in self.keywords:
        self.inputs.add(self._buffer[-1])
      else:
        self._buffer.append(token)
      return self


class GroupState(KeywordState):

  keywords = frozenset(['BY', 'ALL'])
  keywords = frozenset(['PARTITION'])


class JoinState(KeywordState):

  keywords = frozenset(['BY'])
  keywords = frozenset(['PARTITION'])


class MultipleInputState(State):

  pending = True

  def _transition(self, token):
    if self.pending:
      self.inputs.add(token)
      self.pending = False
      return self
    elif token == ',':
      self.pending = True
      return self
    else:
      return WaitState(self)


class SingleInputState(State):

  def _transition(self, token):
    self.inputs = frozenset([token])
    return WaitState(self)


class Operator(object):

  """TODO: Operator docstring.

  :param name: TODO
  :param line: TODO
  :param parents: TODO

  """

  def __init__(self, name, line, parents=None):
    self.name = name
    self.line = line
    self.parents = parents


class Parser(object):

  """Parser."""

  def __init__(self):
    self.available_inputs = {}
    self.operators = []

  def _callback(self, operator, inputs, outputs, line):
    """TODO: _callback docstring.

    :param operator: TODO
    :param inputs: TODO
    :param outputs: TODO

    """
    try:
      parents = {self.available_inputs[input_name] for input_name in inputs}
    except KeyError as err:
      raise PocError('undefined alias %s' % (err, ))
    else:
      operator = Operator(operator, line, parents)
      LOGGER.warn(operator.line)
    for output_name in outputs:
      self.available_inputs[output_name] = operator
    self.operators.append(operator)

  def parse(self, path):
    """Parse pig script.

    :param path: TODO

    """
    operators = []
    state = InitialState()
    with open(path) as reader:
      for _, token, _, _, line in generate_tokens(reader.readline):
        state = state.transition(token, line, self._callback)
    return self.operators
