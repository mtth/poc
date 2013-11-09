#!/usr/bin/env python
# encoding: utf-8

"""Poc."""


import logging


LOGGER = logging.getLogger('root')
LOGGER.setLevel(logging.DEBUG)

LOGGER_STREAM = logging.StreamHandler()
LOGGER_STREAM.setLevel(logging.DEBUG)
LOGGER.addHandler(LOGGER_STREAM)
