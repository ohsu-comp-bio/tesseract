#!/usr/bin/env python

from __future__ import print_function

import os

from setuptools import setup


SETUP_DIR = os.path.dirname(__file__)
README = os.path.join(SETUP_DIR, 'README.md')

setup(
    name='tesseract',
    version='0.1',
    description='Remote code execution with the GA4GH Task Execution API',
    long_description=open(README).read(),
    author='Adam Struck',
    author_email='strucka@ohsu.edu',
    url="https://github.com/ohsu-comp-bio/tesseract",
    download_url="https://github.com/ohsu-comp-bio/tesseract",
    license='MIT',
    install_requires=[
        "attrs>=17.2.0",
        "cloudpickle>=0.3.1",
        "tes>=1.1.1"
    ],
    zip_safe=True
)
