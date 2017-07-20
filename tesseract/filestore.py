from __future__ import absolute_import, print_function

import os
import uuid

from attr import attrs, attrib
from attr.validators import instance_of, optional
from libcloud.storage.providers import get_driver
from urlparse import urlparse

from tesseract.utils import (makedirs, process_url, lookup_provider,
                             lookup_credentials, lookup_region)


@attrs
class FileStore(object):
    bucket = attrib(convert=process_url)
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
    supported = ["s3", "gs", "file"]

    @bucket.validator
    def __validate_bucket(self, attribute, value):
        if not value.endswith("/"):
            raise ValueError(
                "bucket name must end with '/'"
            )
        u = urlparse(value)
        if u.scheme == "file" and u.netloc != "":
            raise ValueError(
                "invalid url"
            )
        if u.scheme not in self.supported:
            raise ValueError(
                "Unsupported scheme - must be one of %s" % (self.supported)
            )
        # self._raise_if_exists(u)

    def __attrs_post_init__(self):
        u = urlparse(self.bucket)
        self.scheme = u.scheme

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
        # self._create_store()

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
            path = urlparse(self.bucket).path
            makedirs(path, exists_ok=False)
        elif self.scheme == "s3":
            self.driver.create_container(self.bucket)
        else:
            raise NotImplementedError(
                "%s scheme is not supported" % (self.scheme)
            )
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

        url = os.path.join(self.bucket, name)

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
