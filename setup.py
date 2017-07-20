#!/usr/bin/env python

from setuptools import setup, find_packages


setup(
    name='tesseract',
    version='0.1.0',
    description='Remote code execution with the GA4GH Task Execution API',
    author='OHSU Computational Biology',
    author_email='CompBio@ohsu.edu',
    maintainer='Adam Struck',
    maintainer_email='strucka@ohsu.edu',
    url="https://github.com/ohsu-comp-bio/tesseract",
    license='MIT',
    packages=find_packages(),
    python_requires='>=2.6, <3',
    install_requires=[
        "attrs>=17.2.0",
        "cloudpickle>=0.3.1",
        "py-tes>=0.1.0"
    ],
    zip_safe=True,
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: MIT License',
        'Topic :: Software Development :: Libraries',
        'Programming Language :: Python :: 2.7',
    ],
)
