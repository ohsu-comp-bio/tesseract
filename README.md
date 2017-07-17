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


def say_hello(s):
    return "hello " + identity(s)


fs = FileStore("./test_store/")
t = Config(fs, "http://localhost:8000")
h = t.remote_call(say_hello, s="world")
result = h.get_result()
print(result)
```

## Resources

* [GA4GH Task Execution Schema](https://github.com/ga4gh/task-execution-schemas)
* [py-tes](https://github.com/ohsu-comp-bio/py-tes)
* [Funnel](https://github.com/ohsu-comp-bio/funnel)
