#!/usr/bin/env python

"""Poc: Pig documentation tool."""

from poc import __version__
from setuptools import find_packages, setup

setup(
    name='poc',
    version=__version__,
    description='Pig documentation tool.',
    long_description=open('README.rst').read(),
    author='Sanketh Katta, Matthieu Monsch',
    author_email='sankethkatta@gmail.com, monsch@alum.mit.edu',
    url='http://github.com/mtth/poc/',
    license='MIT',
    packages=find_packages(),
    classifiers=[
      'Development Status :: 2 - Pre-Alpha',
      'Intended Audience :: Developers',
      'License :: OSI Approved :: MIT License',
      'Programming Language :: Python',
    ],
    install_requires=[
      'docopt',
    ],
    entry_points={'console_scripts': ['poc = poc.__main__:main']},
)
