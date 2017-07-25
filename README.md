[![Build Status](https://travis-ci.org/ohsu-comp-bio/tesseract.svg?branch=master)](https://travis-ci.org/ohsu-comp-bio/tesseract)
[![Coverage Status](https://coveralls.io/repos/github/ohsu-comp-bio/tesseract/badge.svg?branch=master)](https://coveralls.io/github/ohsu-comp-bio/tesseract?branch=master)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

tesseract
======

_tesseract_ is a library that enables the remote execution of python code on 
systems implementing the [GA4GH Task Execution API](https://github.com/ga4gh/task-execution-schemas).


## Quick Start

```
from __future__ import print_function

from tesseract import Tesseract, FileStore


def identity(n):
    return n


def say_hello(a, b):
    return "hello " + identity(a) + b


fs = FileStore("./test_store/")
r = Tesseract(fs, "http://localhost:8000")
r.with_resources(
    cpu_cores=1, ram_gb=4, disk_gb=None, 
    docker="python:2.7", libraries=["cloudpickle"]
)

future = r.run(say_hello, "world", b="!")
result = future.result()
print(result)

r2 = r.clone().with_resources(cpu_cores=4)
f2 = r2.run(say_hello, "more", b="cpus!")
r2 = f2.result()
print(r2)
```

### Object store support

If you provide a swift, s3, or gs bucket url to your `FileStore` _tesseract__ 
will attempt to automatically detect your credentials for each of these systems.

To setup your environment for this run the following commands:

* Google Storage - `gcloud auth application-default login`
    * [Guide](https://cloud.google.com/sdk/gcloud/reference/auth/application-default/)
* Amazon S3 - `aws configure`
    * [Guide](http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html)
* Swift - `source openrc.sh`
    * [Guide](https://access.redhat.com/documentation/en-US/Red_Hat_Enterprise_Linux_OpenStack_Platform/5/html/End_User_Guide/cli_openrc.html)

### Input files

If your function expects input files to be available at a given path then add:

```
r.with_input("s3://your-bucket/path/to/yourfile.txt", "/home/ubuntu/yourfile.txt")
```

The first argument specifies where the file is available, the second specifies where your 
function will expect to find the file. 

### Output files

If your function generates files during its run you can specify these files 
as shown below and _tesseract_ will handle getting them uploaded to the designated bucket. 

```
r.with_output("./relative/path/to/outputfile.txt")
```


## Resources

* [GA4GH Task Execution Schema](https://github.com/ga4gh/task-execution-schemas)
* [py-tes](https://github.com/ohsu-comp-bio/py-tes)
* [Funnel](https://github.com/ohsu-comp-bio/funnel)
