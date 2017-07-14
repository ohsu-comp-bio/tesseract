from __future__ import absolute_import, print_function

import cloudpickle
import pickle
import tes

from attr import attrs, attrib
from attr.validators import instance_of
from collections import Callable
from urlparse import urlparse

from tesla.utils import _create_task


@attrs
class FileStore(object):
    url = attrib()
    path = attrib(init=False)
    protocol = attrib(init=False)

    @url.validator
    def check_url(self, attribute, value):
        supported = ["s3", "gs", "file", ""]
        u = urlparse(value)
        if u.scheme not in supported:
            raise ValueError(
                "Unsupported scheme - must be one of %s" % (supported)
            )

    def __attrs_post_init__(self):
        u = urlparse(self.url)
        if u.scheme == "":
            self.protocol = "file"
        else:
            self.protocol = u.scheme
        self.path = u.path

    def create():
        pass

    def download():
        pass


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
            path = urlparse("file://" + u.path).geturl()
            url = urlparse(self.file_store + "/" + path).geturl()

        return File(url, path)

    def remote_call(self, func, docker=None, cpu_cores=None,
                    ram_gb=None, disk_gb=None, libraries=[], **kwargs):
        runner = RemoteTaskRunner(func, kwargs)
        inputs = []
        for v in kwargs.values():
            if isinstance(v, File):
                inputs.append(v)
        cp_runner = cloudpickle.dumps(runner)
        task_msg = _create_task(
            self.file_store, cp_runner, inputs, docker,
            cpu_cores, ram_gb, disk_gb, libraries
        )
        id = self.tes_client.create_task(task_msg)
        return RemoteTaskHandle(
            id,
            self.file_store.path + "/" + "tesla_result.pickle"
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
    id = attrib(validator=instance_of(str))
    output = attrib(validator=instance_of(str))
    client = attrib(validator=instance_of(tes.HTTPClient))

    def get_result(self):
        r = self.client.wait()
        if r.state != "COMPLETE":
            raise RuntimeError("remote job failed\n%s" % (r))
        # TODO download from file store if using object store
        return pickle.loads(self.output)


class RemoteTaskRunner(object):
    def __init__(self, func, kwargs):
        if not isinstance(func, Callable):
            raise ValueError("func must be callable")
        self.func = func
        self.kwargs = kwargs

    def run(self):
        return self.func(**self.kwargs)
