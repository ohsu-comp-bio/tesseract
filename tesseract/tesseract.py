from __future__ import absolute_import, print_function, unicode_literals

import cloudpickle
import copy
import os
import pkg_resources
import re
import sys
import tes
import uuid
import hashlib

from attr import attrs, attrib, Factory
from attr.validators import instance_of, optional
from builtins import str, bytes
from collections import Callable
from requests.utils import urlparse
from tes.models import strconv

from tesseract.filestore import FileStore, FileExistsError
from tesseract.future import Future, CachedFuture
from tesseract.utils import process_url


@attrs
class Tesseract(object):
    file_store = attrib(validator=instance_of(FileStore))
    tes_url = attrib(
        default="http://localhost:8000",
        convert=strconv,
        validator=instance_of(str)
    )
    timeout = attrib(default=30, validator=instance_of(int))
    input_files = attrib(
        default=Factory(list), validator=tes.models.list_of(tes.TaskParameter)
    )
    output_files = attrib(
        default=Factory(list), validator=tes.models.list_of(tes.TaskParameter)
    )
    cpu_cores = attrib(default=None, validator=optional(instance_of(int)))
    ram_gb = attrib(
        default=None, validator=optional(instance_of((int, float)))
    )
    disk_gb = attrib(
        default=None, validator=optional(instance_of((int, float)))
    )
    docker = attrib(
        convert=strconv, validator=instance_of(str)
    )
    libraries = attrib(
        convert=strconv, validator=tes.models.list_of(str)
    )
    cache_name = attrib(
        default=None, convert=strconv, validator=optional(instance_of(str))
    )
    __tes_client = attrib(init=False,  validator=instance_of(tes.HTTPClient))
    __id = attrib(init=False, convert=strconv, validator=instance_of(str))

    @docker.default
    def __default_docker(self):
        v = sys.version_info
        return "python:%s.%s.%s" % (v.major, v.minor, v.micro)

    @libraries.default
    def __default_libraries(self):
        return ["cloudpickle"]

    @tes_url.validator
    def __validate_tes_url(self, attribute, value):
        value = urlparse(value).geturl()
        if not isinstance(value, str):
            raise ValueError("%s must be a valid URL" % (attribute))

    def __attrs_post_init__(self):
        self.__tes_client = tes.HTTPClient(self.tes_url, timeout=self.timeout)
        self.__id = None

    def __get_id(self):
        # enable call caching by hardcoding the id attribute on the instance.
        if self.cache_name is not None:
            if self.__id is None:
                self.__id = self.cache_name

            # id is set meaning with_input, with_output, with_upload or
            # run has been called. For any of these cases we set id to the
            # cache name and adjust the input / output urls.
            elif self.__id != self.cache_name:
                # adjust urls for inputs and outputs for cached run
                if self.input_files is not None:
                    for i in self.input_files:
                        i.url = re.sub(self.__id, self.cache_name, i.url)
                if self.output_files is not None:
                    for o in self.output_files:
                        o.url = re.sub(self.__id, self.cache_name, o.url)

                self.__id = self.cache_name

        # no call caching - the id is reset if it is present in the FileStore
        elif self.__id is None or self.file_store.exists(self.__id,
                                                         type="directory"):
            self.__id = "tesseract_%s" % (uuid.uuid4().hex)

        return self.__id

    def clone(self):
        return copy.deepcopy(self)

    def with_resources(self, cpu_cores=None, ram_gb=None, disk_gb=None,
                       docker=None, libraries=None):
        # only override if set
        self.cpu_cores = cpu_cores or self.cpu_cores
        self.ram_gb = ram_gb or self.ram_gb
        self.disk_gb = disk_gb or self.disk_gb
        self.docker = docker or self.docker
        if libraries is not None:
            self.libraries = libraries

    def with_call_caching(self, name):
        self.cache_name = name

    def with_upload(self, path):
        run_id = self.__get_id()
        name = os.path.join(run_id, os.path.basename(path))
        try:
            input_url = self.file_store.upload(
                path=path, name=name, overwrite_existing=False
            )
        except FileExistsError:
            input_url = self.file_store.generate_url(name)
            print("warning: file [%s] was not uploaded." % (path),
                  "It already exists in the FileStore [%s]" % (input_url))
        return self.with_input(input_url, path)

    def with_input(self, url, path):
        u = urlparse(process_url(url))
        if u.scheme not in self.file_store.supported:
            raise ValueError(
                "Unsupported scheme - must be one of %s" %
                (self.file_store.supported)
            )
        if u.scheme == "file" and self.file_store.scheme != "file":
            raise ValueError("please upload your input file to the file store")

        u = urlparse(path)
        if u.scheme not in ["file", ""]:
            raise ValueError("runtime path must be defined as a local path")
        if u.scheme == "file":
            path = os.path.join(u.netloc, u.path)

        if path.startswith("./"):
            pass
        elif not os.path.isabs(path):
            raise ValueError(
                "runtime path must be an absolute path or start with './'"
            )

        self.input_files.append(
            tes.TaskParameter(
                path=os.path.join("/tmp/tesseract", re.sub("^./", "", path)),
                url=url,
                type="FILE"
            )
        )

    def with_output(self, path):
        u = urlparse(path)
        if u.scheme not in ["file", ""]:
            raise ValueError("runtime path must be defined as a local path")
        if u.scheme == "file":
            path = os.path.join(u.netloc, u.path)

        if path.startswith("./"):
            pass
        elif not os.path.isabs(path):
            raise ValueError(
                "runtime path must be an absolute path or start with './'"
            )

        run_id = self.__get_id()
        self.output_files.append(
            tes.TaskParameter(
                path=os.path.join("/tmp/tesseract", re.sub("^./", "", path)),
                url=self.file_store.generate_url(
                    os.path.join(run_id, re.sub("^./|^/", "", path))
                ),
                type="FILE"
            )
        )

    def run(self, func, *args, **kwargs):
        if not isinstance(func, Callable):
            raise TypeError("func not an instance of collections.Callable")

        run_id = self.__get_id()

        # serialize function and arguments
        # upload to object store if necessary
        runner = {"func": func, "args": args, "kwargs": kwargs}
        cp_str = cloudpickle.dumps(runner)

        m = hashlib.sha256()
        m.update(cp_str)
        mhex = m.hexdigest()
        input_name = os.path.join(run_id, "tesseract_func_%s.pickle" % (mhex))
        output_name = os.path.join(run_id, "tesseract_res_%s.pickle" % (mhex))

        if self.file_store.exists(input_name, type="file"):
            input_cp_url = self.file_store.generate_url(input_name)
            print("Found cached input: %s" % (input_cp_url))
        else:
            input_cp_url = self.file_store.upload(
                name=input_name, contents=cp_str
            )

        # define storage url for pickled output
        output_cp_url = self.file_store.generate_url(output_name)
        if self.file_store.exists(output_name, type="file"):
            print("Found cached output: %s" % (output_cp_url))
            return CachedFuture(
                output_name,
                FileStore(self.file_store.url),
            )

        # create task msg and submit
        task_msg = self._create_task_msg(input_cp_url, output_cp_url)
        id = self.__tes_client.create_task(task_msg)
        return Future(
            id,
            output_name,
            FileStore(self.file_store.url),
            self.__tes_client
        )

    def _create_task_msg(self, input_cp_url, output_cp_url):
        runner = pkg_resources.resource_string(
            __name__, "resources/runner.py"
        )
        if isinstance(runner, bytes):
            runner = runner.decode("utf8")

        cmd_install_reqs = "pip install %s" % (" ".join(self.libraries))
        cmd_tesseract = "python tesseract.py func.pickle"

        if len(self.libraries) == 0:
            cmd = cmd_tesseract
        else:
            cmd = cmd_install_reqs + " && " + cmd_tesseract

        task = tes.Task(
            name="tesseract remote execution",
            inputs=self.input_files + [
                tes.TaskParameter(
                    name="pickled function",
                    url=input_cp_url,
                    path="/tmp/tesseract/func.pickle",
                    type="FILE"
                ),
                tes.TaskParameter(
                    name="tesseract runner script",
                    path="/tmp/tesseract/tesseract.py",
                    type="FILE",
                    contents=str(runner)
                )
            ],
            outputs=self.output_files + [
                tes.TaskParameter(
                    name="pickled result",
                    url=output_cp_url,
                    path="/tmp/tesseract/result.pickle",
                    type="FILE"
                )
            ],
            resources=tes.Resources(
                cpu_cores=self.cpu_cores,
                ram_gb=self.ram_gb,
                size_gb=self.disk_gb
            ),
            executors=[
                tes.Executor(
                    image_name=self.docker,
                    cmd=["sh", "-c", cmd],
                    stdout="/tmp/tesseract/stdout",
                    stderr="/tmp/tesseract/stderr",
                    workdir="/tmp/tesseract"
                )
            ]
        )
        return task
