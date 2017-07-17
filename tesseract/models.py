from __future__ import absolute_import, print_function

import cloudpickle
import os
import tes
import uuid

from attr import attrs, attrib
from attr.validators import instance_of
from collections import Callable
from urlparse import urlparse

from tesseract.utils import _create_task, makedirs, process_url


@attrs
class FileStore(object):
    url = attrib(convert=process_url)
    scheme = attrib(init=False, validator=instance_of(str))
    supported = ["s3", "gs", "file"]

    @url.validator
    def check_url(self, attribute, value):
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

    def create(self, path=None, contents=None):
        if path is not None and contents is not None:
            raise RuntimeError("Cannot provide both local path and contents")
        if path is None and contents is None:
            raise RuntimeError("Provide either local path or contents")

        if self.scheme == "file":
            if path is None:
                url = os.path.join(self.url, str(uuid.uuid4()))
            else:
                url = os.path.join(self.url, os.path.basname(path))
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
            raise NotImplementedError()
        return path


@attrs
class Config(object):
    file_store = attrib(validator=instance_of(FileStore))
    tes_url = attrib(default="localhost:8000")
    tes_client = attrib(init=False)

    @tes_url.validator
    def check_url(self, attribute, value):
        value = urlparse(value).geturl()
        if not isinstance(value, str):
            raise ValueError("%s must be a valid URL" % (attribute))

    def __attrs_post_init__(self):
        self.tes_client = tes.HTTPClient(self.tes_url)

    def file(self, path):
        u = urlparse(path)
        path = u.path
        url = u.geturl()
        if u.scheme == "":
            p = os.path.abspath(os.path.join(u.netloc, u.path))
            path = urlparse("file://" + p).geturl()
            url = urlparse(self.file_store + "/" + path).geturl()

        return File(url, path)

    def remote_call(self, func, docker=None, cpu_cores=None,
                    ram_gb=None, disk_gb=None, libraries=[],
                    **kwargs):
        if not isinstance(func, Callable):
            raise TypeError("func not an instance of collections.Callable")
        runner = {"func": func, "kwargs": kwargs}
        cp_str = cloudpickle.dumps(runner)
        input_cp_url = self.file_store.create(contents=cp_str)
        output_cp_url = urlparse(
            self.file_store.url + "/" + "tesseract_result.pickle"
        ).geturl()
        inputs = []
        for v in kwargs.values():
            if isinstance(v, File):
                inputs.append(v)
        task_msg = _create_task(
            input_cp_url, output_cp_url, inputs, docker,
            cpu_cores, ram_gb, disk_gb, libraries
        )
        id = self.tes_client.create_task(task_msg)
        return RemoteTaskHandle(
            id,
            output_cp_url,
            self.file_store,
            self.tes_client
        )


@attrs
class File(object):
    url = attrib(validator=instance_of(str))
    path = attrib(validator=instance_of(str))
    upload = attrib(init=False)

    def __attrs_post_init__(self):
        us = urlparse(self.url).scheme
        ps = urlparse(self.path).scheme
        self.upload = us != ps and us != "file"


@attrs
class RemoteTaskHandle(object):
    id = attrib(convert=str, validator=instance_of(str))
    output = attrib(validator=instance_of(str))
    file_store = attrib(validator=instance_of(FileStore))
    client = attrib(validator=instance_of(tes.HTTPClient))

    def get_result(self):
        r = self.client.wait(self.id)
        if r.state != "COMPLETE":
            raise RuntimeError("remote job failed:\n%s" % (r))
        path = self.file_store.download(self.output)
        return cloudpickle.load(open(path, "rb"))
