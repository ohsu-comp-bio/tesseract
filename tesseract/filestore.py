from __future__ import absolute_import, print_function, unicode_literals


import os
import re
import shutil
import six
import tempfile
import uuid

from attr import attrs, attrib
from attr.validators import instance_of, optional
from builtins import str, bytes
from libcloud.storage.providers import get_driver
from libcloud.storage.types import ObjectDoesNotExistError
from io import open
from tes.models import strconv
from requests.utils import urlparse

from tesseract.utils import (makedirs, process_url, lookup_provider,
                             lookup_credentials, lookup_region,
                             lookup_project)


@attrs
class FileStore(object):
    url = attrib(convert=process_url)
    key = attrib(
        default=None, convert=strconv, validator=optional(instance_of(str))
    )
    secret = attrib(
        default=None, convert=strconv, validator=optional(instance_of(str))
    )
    region = attrib(
        default=None, convert=strconv, validator=optional(instance_of(str))
    )
    project = attrib(
        default=None, convert=strconv, validator=optional(instance_of(str))
    )
    swift_force_auth_url = attrib(
        default=None, convert=strconv, validator=optional(instance_of(str))
    )
    swift_force_auth_version = attrib(
        default='2.0_password',
        convert=strconv,
        validator=optional(instance_of(str))
    )
    swift_tenant_name = attrib(
        default=None, convert=strconv, validator=optional(instance_of(str))
    )
    provider = attrib(
        init=False, default=None, validator=optional(instance_of(object))
    )
    driver = attrib(
        init=False, default=None, validator=optional(instance_of(object))
    )
    scheme = attrib(
        init=False, default="", convert=strconv, validator=instance_of(str)
    )
    bucket = attrib(
        init=False, default="", convert=strconv, validator=instance_of(str)
    )
    path = attrib(
        init=False, default="", convert=strconv, validator=instance_of(str))
    supported = attrib(init=False, default=["file", "gs", "s3", "swift"])

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

    def __attrs_post_init__(self):
        u = urlparse(self.url)
        self.scheme = u.scheme
        self.bucket = u.netloc

        if self.scheme == "file":
            self.path = re.sub("/$", "", u.path)
        else:
            self.path = re.sub("^/|/$", "", u.path)

        if self.scheme != "file":
            self.provider = get_driver(
                lookup_provider(self.scheme, self.region)
            )

            if self.key is None and self.secret is None:
                self.key, self.secret = lookup_credentials(self.scheme)

            # Openstack Swift
            if self.scheme == "swift":
                try:
                    auth_url = urlparse(os.environ['OS_AUTH_URL'])
                    self.swift_force_auth_url = "%s://%s" % (auth_url.scheme,
                                                             auth_url.netloc)
                    self.swift_tenant_name = os.environ['OS_TENANT_NAME']
                except Exception:
                    raise ValueError(
                        "OS_AUTH_URL and/or OS_TENANT_NAME were not found"
                    )
                self.driver = self.provider(
                    key=self.key,
                    secret=self.secret,
                    ex_force_auth_url=self.swift_force_auth_url,
                    ex_tenant_name=self.swift_tenant_name,
                    ex_force_auth_version=self.swift_force_auth_version,
                )

            # Google Storage or Amazon S3
            else:
                if self.region is None:
                    self.region = lookup_region(self.scheme)

                if self.project is None:
                    self.project = lookup_project(self.scheme)

                self.driver = self.provider(
                    key=self.key,
                    secret=self.secret,
                    region=self.region,
                    project=self.project
                )

        self.key = None
        self.secret = None
        self.__create_store()

    def __create_store(self):
        if self.scheme == "file":
            makedirs(self.path, exists_ok=True)
        else:
            try:
                self.driver.get_container(self.bucket)
            except Exception:
                self.driver.create_container(self.bucket)
        return

    def __delete_bucket(self):
        if self.scheme == "file":
            shutil.rmtree(self.bucket)
        else:
            self.driver.delete_container(self.bucket)
        return

    def generate_url(self, name):
        return "%s://%s" % (
            self.scheme, os.path.join(self.bucket, self.path, name)
        )

    def exists(self, name, type="file"):
        if type.lower() in ["file", "f"]:
            return self._file_exists(name)
        elif type.lower() in ["directory", "dir", "d"]:
            return self._directory_exists(name)
        else:
            raise ValueError("type must be one of: ['file', 'directory']")

    def _file_exists(self, name):
        found = False
        if self.scheme == "file":
            for root, dnames, fnames in os.walk(self.path):
                if root != self.path:
                    fnames = [
                        os.path.join(os.path.basename(root), f) for f in fnames
                    ]
                if name in fnames:
                    found = True
        else:
            objs = self.driver.list_container_objects(
                self.driver.get_container(self.bucket)
            )
            for o in objs:
                if os.path.join(self.path, name) == o.name:
                    found = True

        return found

    def _directory_exists(self, name):
        found = False
        if self.scheme == "file":
            for root, dnames, fnames in os.walk(self.path):
                if name in dnames:
                    found = True
        else:
            objs = self.driver.list_container_objects(
                self.driver.get_container(self.bucket)
            )
            for o in objs:
                if os.path.join(self.path, name) in o.name:
                    found = True

        return found

    def upload(self, path=None, name=None, contents=None,
               overwrite_existing=False):
        if path is not None and contents is not None:
            raise ValueError("Cannot provide both local path and contents")
        if path is None and contents is None:
            raise ValueError("Provide either local path or contents")

        if name is None:
            if path is None:
                name = os.path.basname(path)
            else:
                name = str("tmp_" + uuid.uuid4().hex)

        if path is not None:
            contents = open(path, "r").read()

        if isinstance(contents, six.string_types):
            contents = bytes(contents, "utf8")

        url = os.path.join(self.path, name)

        if self.scheme == "file":
            if os.path.exists(url) and not overwrite_existing:
                raise FileExistsError(self.generate_url(name))
            makedirs(os.path.dirname(url), exists_ok=True)
            with open(url, "wb") as fh:
                fh.write(contents)
        else:
            try:
                key = u"%s" % (os.path.join(self.path, name))
                self.driver.get_object(
                    self.bucket, key
                )
                if not overwrite_existing:
                    raise FileExistsError(self.generate_url(name))
            except ObjectDoesNotExistError:
                pass

            tmpf = tempfile.NamedTemporaryFile(mode="wb", delete=False)
            tmpf.write(contents)
            tmpf.close()
            self.driver.upload_object(
                file_path=tmpf.name,
                container=self.driver.get_container(self.bucket),
                object_name=url
            )
            os.remove(tmpf.name)
            return self.scheme + "://" + self.bucket + "/" + url

        return url

    def download(self, name, destination_path, overwrite_existing=False):
        if os.path.exists(destination_path) and not overwrite_existing:
            raise FileExistsError(destination_path)

        if self.scheme == "file":
            shutil.copyfile(os.path.join(self.path, name), destination_path)
        else:
            key = u"%s" % (os.path.join(self.path, name))
            obj = self.driver.get_object(
                self.bucket, key
            )
            self.driver.download_object(
                obj,
                destination_path,
                overwrite_existing
            )
        return destination_path


class FileExistsError(Exception):
    def __init__(self, file):
        super(FileExistsError, self).__init__(
            "file [%s] exists; to force set overwrite_existing to True" %
            (file)
        )
