[![Build Status](https://travis-ci.org/ohsu-comp-bio/tesseract.svg?branch=master)](https://travis-ci.org/ohsu-comp-bio/tesseract)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

tesseract
======

_tesseract_ is a library that enables remote execution of python code. 


## Quick Start

```
from __future__ import print_function

from tesseract import Config, FileStore


def identity(n):
    return n


def say_hello(a, b):
    return "hello " + identity(a) + b


fs = FileStore("./test_store/")
r = Config(fs, "http://localhost:8000")
r = r.resource_request(
    cpu_cores=1, ram_gb=4, disk_gb=None, 
    docker="python:2.7", libraries=["cloudpickle", "scipy"]
)
future = r.remote_call(say_hello, "!", b="world")
result = future.result()
print(result)
```

## Resources

* [GA4GH Task Execution Schema](https://github.com/ga4gh/task-execution-schemas)
* [py-tes](https://github.com/ohsu-comp-bio/py-tes)
* [Funnel](https://github.com/ohsu-comp-bio/funnel)
