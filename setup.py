#!/usr/bin/env python

import io
import os
import re

from setuptools import setup


def read(*names, **kwargs):
    with io.open(
        os.path.join(os.path.dirname(__file__), *names),
        encoding=kwargs.get("encoding", "utf8")
    ) as fp:
        return fp.read()


def find_version(*file_paths):
    version_file = read(*file_paths)
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                              version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")


setup(
    name='py-tesseract',
    version=find_version("tesseract", "__init__.py"),
    description='Remote code execution with the GA4GH Task Execution API',
    author='OHSU Computational Biology',
    author_email='CompBio@ohsu.edu',
    maintainer='Adam Struck',
    maintainer_email='strucka@ohsu.edu',
    url="https://github.com/ohsu-comp-bio/tesseract",
    license='MIT',
    packages=["tesseract"],
    package_data={"tesseract": ["resources/*.py"]},
    python_requires='>=2.6, <4',
    install_requires=[
        "apache_libcloud>=2.2.1",
        "attrs>=17.2.0",
        "cloudpickle>=0.4.1",
        "future>=0.16.0",
        "futures>=3.1.1",
        "py-tes>=0.1.6",
        "requests>=2.18.1"
    ],
    zip_safe=True,
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: MIT License',
        'Topic :: Software Development :: Libraries',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
)
