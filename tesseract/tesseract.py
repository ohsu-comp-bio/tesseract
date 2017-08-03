from __future__ import absolute_import, print_function, unicode_literals

import cloudpickle
import copy
import os
import pkg_resources
import re
import sys
import tempfile
import tes
import uuid

from attr import attrs, attrib, Factory
from attr.validators import instance_of, optional
from builtins import str, bytes
from collections import Callable
from concurrent.futures import ThreadPoolExecutor
from io import open
from requests.utils import urlparse
from tes.models import strconv

from tesseract.filestore import FileStore
from tesseract.utils import process_url


@attrs
class Tesseract(object):
    file_store = attrib(validator=instance_of(FileStore))
    tes_url = attrib(default="http://localhost:8000", convert=strconv)
    __tes_client = attrib(init=False,  validator=instance_of(tes.HTTPClient))
    __id = attrib(init=False, convert=strconv, validator=instance_of(str))
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
        self.__tes_client = tes.HTTPClient(self.tes_url)
        self.__id = None

    def __get_id(self):
        if self.__id is None or self.file_store.exists(self.__id):
            self.__id = "tesseract_%s" % (uuid.uuid4().hex)
        return self.__id

    def clone(self):
        return copy.deepcopy(self)

    def with_resources(self, cpu_cores=None, ram_gb=None, disk_gb=None,
                       docker=None, libraries=[]):
        # only override if set
        self.cpu_cores = cpu_cores or self.cpu_cores
        self.ram_gb = ram_gb or self.ram_gb
        self.disk_gb = disk_gb or self.disk_gb
        self.docker = docker or self.docker
        self.libraries = libraries or self.libraries

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
        input_name = os.path.join(run_id, "tesseract_func.pickle")
        input_cp_url = self.file_store.upload(name=input_name, contents=cp_str)

        # define storage url for pickled output
        output_cp_url = self.file_store.generate_url(
            "%s/tesseract_result.pickle" % (run_id)
        )

        # create task msg and submit
        task_msg = self._create_task_msg(input_cp_url, output_cp_url)
        id = self.__tes_client.create_task(task_msg)
        return Future(
            id,
            output_cp_url,
            self.file_store,
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


@attrs
class Future(object):
    __id = attrib(convert=strconv, validator=instance_of(str))
    __output_url = attrib(convert=strconv, validator=instance_of(str))
    __file_store = attrib(validator=instance_of(FileStore))
    __client = attrib(validator=instance_of(tes.HTTPClient))
    __execption = attrib(
        init=False, default=None, validator=optional(instance_of(Exception))
    )
    __result = attrib(init=False, default=None)

    def __attrs_post_init__(self):
        pool = ThreadPoolExecutor(1)
        self.__result = pool.submit(self.__poll)

    def __poll(self):
        r = self.__client.wait(self.__id)
        if r.state != "COMPLETE":
            r = self.__client.get_task(self.__id, "FULL")
            raise RuntimeError("remote job failed:\n%s" % (r))

        tmp = tempfile.NamedTemporaryFile(delete=False)
        tmp.close()
        self.__file_store.download(self.__output_url, tmp.name, True)
        return cloudpickle.load(open(tmp.name, "rb"))

    def result(self, timeout=None):
        return self.__result.result(timeout=timeout)

    def exeception(self, timeout=None):
        return self.__result.exception(timeout=timeout)

    def running(self):
        r = self.__client.get_task(self.__id, "MINIMAL")
        return r.state in["QUEUED", "INITIALIZING", "RUNNING"]

    def done(self):
        r = self.__client.get_task(self.__id, "MINIMAL")
        return r.state in ["COMPLETE", "ERROR", "SYSTEM_ERROR", "CANCELED"]

    def cancel(self):
        self.__result.cancel()
        self.__client.cancel(self.__id)
        return True

    def cancelled(self):
        r = self.__client.get_task(self.__id, "MINIMAL")
        return r.state == "CANCELED"
