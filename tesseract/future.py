from __future__ import absolute_import, print_function, unicode_literals

import cloudpickle
import tempfile
import tes

from attr import attrs, attrib
from attr.validators import instance_of, optional
from builtins import str
from concurrent.futures import ThreadPoolExecutor
from io import open
from tes.models import strconv

from tesseract.filestore import FileStore


@attrs
class Future(object):
    __id = attrib(convert=strconv, validator=instance_of(str))
    __output_key = attrib(convert=strconv, validator=instance_of(str))
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
        self.__file_store.download(self.__output_key, tmp.name, True)
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


@attrs
class CachedFuture(object):
    __output_key = attrib(convert=strconv, validator=instance_of(str))
    __file_store = attrib(validator=instance_of(FileStore))
    __result = attrib(init=False, default=None)

    def __attrs_post_init__(self):
        pool = ThreadPoolExecutor(1)
        self.__result = pool.submit(self.__download)

    def __download(self):
        tmp = tempfile.NamedTemporaryFile(delete=False)
        tmp.close()
        self.__file_store.download(self.__output_key, tmp.name, True)
        return cloudpickle.load(open(tmp.name, "rb"))

    def result(self, timeout=None):
        return self.__result.result(timeout=timeout)

    def exeception(self, timeout=None):
        return self.__result.exception(timeout=timeout)

    def running(self):
        return self.__result is None

    def done(self):
        return self.__result is not None

    def cancel(self):
        return False

    def cancelled(self):
        return False
