from __future__ import absolute_import, print_function

import cloudpickle
import os
import tes
import uuid

from attr import attrs, attrib, evolve
from attr.validators import instance_of, optional
from collections import Callable
from concurrent.futures import ThreadPoolExecutor
from urlparse import urlparse

from tesseract.utils import makedirs, process_url


@attrs
class FileStore(object):
    url = attrib(convert=process_url)
    scheme = attrib(init=False, validator=instance_of(str))
    supported = ["s3", "gs", "file"]

    @url.validator
    def __validate_url(self, attribute, value):
        u = urlparse(value)
        if u.scheme == "file" and u.netloc != "":
            raise ValueError(
                "invalid url"
            )
        if u.scheme not in self.supported:
            raise ValueError(
                "Unsupported scheme - must be one of %s" % (self.supported)
            )
        self._raise_if_exists(u)

    def __attrs_post_init__(self):
        u = urlparse(self.url)
        self.scheme = u.scheme
        self._create_store()

    @staticmethod
    def _raise_if_exists(u):
        if u.scheme == "file":
            path = u.path
        else:
            raise NotImplementedError(
                "%s scheme is not supported" % (u.scheme)
            )

        if os.path.exists(path):
            raise ValueError(
                "FileStore already exists: %s" % (path)
            )
        return

    def _create_store(self):
        if self.scheme == "file":
            path = urlparse(self.url).path
        else:
            raise NotImplementedError(
                "%s scheme is not supported" % (self.scheme)
            )

        makedirs(path, exists_ok=False)

        return

    def upload(self, path=None, name=None, contents=None):
        if path is not None and contents is not None:
            raise RuntimeError("Cannot provide both local path and contents")
        if path is None and contents is None:
            raise RuntimeError("Provide either local path or contents")

        if name is None:
            if path is None:
                name = os.path.basname(path)
            else:
                name = str("tmp" + uuid.uuid4().hex)

        url = os.path.join(self.url, name)

        if self.scheme == "file":
            if path is not None:
                contents = open(path, "r").read()
            with open(urlparse(url).path, "w") as fh:
                fh.write(contents)
        else:
            raise NotImplementedError(
                "%s scheme is not supported" % (self.scheme)
            )

        return url

    def download(self, url):
        u = urlparse(url)
        if u.scheme == "file":
            path = u.path
        else:
            raise NotImplementedError(
                "%s scheme is not supported" % (self.scheme)
            )
        return path


@attrs
class Tesseract(object):
    file_store = attrib(validator=instance_of(FileStore))
    tes_url = attrib(default="localhost:8000")
    __tes_client = attrib(init=False)
    input_files = attrib(default=[], validator=tes.models.list_of(str))
    output_files = attrib(default=[], validator=tes.models.list_of(str))
    cpu_cores = attrib(default=None, validator=optional(instance_of(int)))
    ram_gb = attrib(
        default=None, validator=optional(instance_of((int, float)))
    )
    disk_gb = attrib(
        default=None, validator=optional(instance_of((int, float)))
    )
    docker = attrib(default="python:2.7", validator=optional(instance_of(str)))
    libraries = attrib(
        default=["cloudpickle"], validator=tes.models.list_of(str)
    )

    @tes_url.validator
    def __validate_tes_url(self, attribute, value):
        value = urlparse(value).geturl()
        if not isinstance(value, str):
            raise ValueError("%s must be a valid URL" % (attribute))

    def __attrs_post_init__(self):
        self.__tes_client = tes.HTTPClient(self.tes_url)

    def with_resources(self, cpu_cores=None, ram_gb=None, disk_gb=None,
                       docker=None, libraries=[]):
        # only override if set
        c = cpu_cores or self.cpu_cores
        r = ram_gb or self.ram_gb
        d = disk_gb or self.disk_gb
        i = docker or self.docker
        l = libraries or self.libraries
        return evolve(
            self, cpu_cores=c, ram_gb=r, disk_gb=d,
            docker=i, libraries=l
        )

    def with_input(self, url, path):
        u = urlparse(url)
        if u.scheme == "":
            u = urlparse("file://" + os.path.abspath(path))
        if u.scheme == "file" and self.file_store.scheme != "file":
            raise ValueError("please upload your input file to the file store")

        u = urlparse(path)
        if u.scheme not in ["file", ""]:
            raise ValueError("runtime path must be a local path")

        if path.startswith("./"):
            path = os.path.join(
                "file:///tmp/tesseract/", path.strip("./")
            )

        self.input_files = tes.TaskParameter(
            path=path,
            url=url,
            type="FILE"
        )

    def with_output(self, path):
        if not path.startswith("./"):
            raise ValueError(
                "output paths must start with './'"
            )
        self.output_files.append(
            tes.TaskParameter(
                path=os.path.join(
                    "file:///tmp/tesseract/outputs", path.strip("./")
                ),
                url=urlparse(
                    self.file_store.url + "/" + path.strip("./")
                ).geturl(),
                type="FILE"
            )
        )

    def run(self, func, *args, **kwargs):
        if not isinstance(func, Callable):
            raise TypeError("func not an instance of collections.Callable")
        run_id = uuid.uuid4().hex

        # serialize function and arguments
        # upload to object store if necessary
        runner = {"func": func, "args": args, "kwargs": kwargs}
        cp_str = cloudpickle.dumps(runner)
        input_name = "tesseract_func_%s.pickle" % (run_id)
        input_cp_url = self.file_store.upload(name=input_name, contents=cp_str)
        self.input_files.append(
            tes.TaskParameter(
                name="pickled function",
                url=input_cp_url,
                path="/tmp/tesseract/func.pickle",
                type="FILE"
            )
        )

        # define storage url for pickled output
        output_name = "%s/tesseract_result_%s.pickle" % (self.file_store.url,
                                                         run_id)
        output_cp_url = urlparse(output_name).geturl()
        self.outputs.append(
            tes.TaskParameter(
                name="pickled result",
                url=output_cp_url,
                path="/tmp/tesseract/result.pickle",
                type="FILE"
            )
        )

        # create task msg and submit
        task_msg = self._create_task()
        id = self.tes_client.create_task(task_msg)
        return Future(
            id,
            output_cp_url,
            self.file_store,
            self.tes_client
        )

    def _create_task(self):
        runner = os.path.join(
            os.path.dirname(__file__), "resources", "runner.py"
        )

        cmd_install_reqs = "pip install %s" % (" ".join(self.libraries))
        cmd_tesseract = "python tesseract.py func.pickle"

        if self.docker is None:
            docker = "python:2.7"

        if len(self.libraries) == 0:
            cmd = cmd_tesseract
        else:
            cmd = cmd_install_reqs + " && " + cmd_tesseract

        task = tes.Task(
            name="tesseract remote execution",
            inputs=[
                tes.TaskParameter(
                    name="tesseract runner script",
                    path="/tmp/tesseract/tesseract.py",
                    type="FILE",
                    contents=open(runner, "r").read()
                )
            ] + self.input_files,
            outputs=self.output_files,
            resources=tes.Resources(
                cpu_cores=self.cpu_cores,
                ram_gb=self.ram_gb,
                size_gb=self.disk_gb
            ),
            executors=[
                tes.Executor(
                    image_name=docker,
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
    __id = attrib(convert=str, validator=instance_of(str))
    __output_url = attrib(validator=instance_of(str))
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
        path = self.__file_store.download(self.__output_url)
        return cloudpickle.load(open(path, "rb"))

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
