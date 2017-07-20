from __future__ import absolute_import, print_function

import os
import re
import shutil
import tempfile
import uuid

from attr import attrs, attrib
from attr.validators import instance_of, optional
from libcloud.storage.providers import get_driver
from urlparse import urlparse

from tesseract.utils import (makedirs, process_url, lookup_provider,
                             lookup_credentials, lookup_region)


@attrs
class FileStore(object):
    filestore_url = attrib(convert=process_url)
    key = attrib(default=None, validator=optional(instance_of(str)))
    secret = attrib(default=None, validator=optional(instance_of(str)))
    secure = attrib(default=True, validator=instance_of(bool))
    host = attrib(default=None, validator=optional(instance_of(str)))
    port = attrib(default=None, validator=optional(instance_of(int)))
    api_version = attrib(default=None, validator=optional(instance_of(str)))
    region = attrib(default=None, validator=optional(instance_of(str)))
    provider = attrib(init=False)
    driver = attrib(init=False)
    scheme = attrib(init=False, validator=instance_of(str))
    __bucket = attrib(init=False, validator=instance_of(str))
    __path = attrib(init=False, validator=instance_of(str))
    supported = ["s3", "gs", "file"]

    @filestore_url.validator
    def __validate_filestore_url(self, attribute, value):
        u = urlparse(value)
        if u.scheme == "file" and u.netloc != "":
            raise ValueError(
                "invalid url"
            )
        if u.scheme not in self.supported:
            raise ValueError(
                "Unsupported scheme - must be one of %s" % (self.supported)
            )

    def __attrs_post_init__(self):
        u = urlparse(self.filestore_url)
        self.scheme = u.scheme
        self.__bucket = u.netloc
        self.__path = re.sub("/$", "", u.path)

        if self.scheme != "file":
            if self.region is None:
                self.region = lookup_region(self.scheme)

            self.provider = get_driver(
                lookup_provider(self.scheme, self.region)
            )

            if self.key is None and self.secret is None:
                self.key, self.secret = lookup_credentials(self.scheme)

            self.driver = self.provider(
                key=self.key,
                secret=self.secret,
                secure=self.secure,
                host=self.host,
                port=self.port,
                api_version=self.api_version,
                region=self.region
            )

        self.key = None
        self.secret = None

        self._create_store()

    def __create_store(self):
        if self.scheme == "file":
            makedirs(self.__path, exists_ok=True)
        else:
            try:
                self.driver.get_container(self.__bucket)
            except:
                self.driver.create_container(self.__bucket)
        return

    def __delete_bucket(self):
        if self.scheme == "file":
            shutil.rmtree(self.__bucket)
        else:
            self.driver.delete_container(self.__bucket)
        return

    def upload(self, path=None, name=None, contents=None,
               overwrite_existing=False):
        if path is not None and contents is not None:
            raise RuntimeError("Cannot provide both local path and contents")
        if path is None and contents is None:
            raise RuntimeError("Provide either local path or contents")

        if name is None:
            if path is None:
                name = os.path.basname(path)
            else:
                name = str("tmp" + uuid.uuid4().hex)

        if path is not None:
            contents = open(path, "r").read()

        url = os.path.join(self.__path, name)

        if self.scheme == "file":
            with open(url, "w") as fh:
                fh.write(contents)
        else:
            tmpf = tempfile.NamedTemporaryFile(delete=False)
            tmpf.write(contents)
            tmpf.close()
            self.driver.upload_object(
                file_path=path,
                container=self.driver.get_container(self.__bucket),
                object_name=url
            )
            os.remove(tmpf.name)

        return url

    def download(self, url, destination_path, overwrite_existing=False):
        if os.path.exists(destination_path) and not overwrite_existing:
            raise IOError(
                "destination_path exists, and overwrite_existing = False"
            )
        u = urlparse(url)
        source = u.path
        if u.scheme == "file":
            shutil.copyFile(source, destination_path)
        else:
            obj = self.driver.get_object(self.__bucket, source)
            self.driver.download_object(
                obj,
                destination_path,
                overwrite_existing
            )
        return destination_path
