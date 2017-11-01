from __future__ import absolute_import, print_function, unicode_literals

import errno
import json
import os
import re

from libcloud.storage.types import Provider
from io import open
from tes.models import strconv
from requests.utils import urlparse


def makedirs(path, exists_ok=True):
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            if exists_ok:
                pass
            else:
                raise exc
        else:
            raise exc


def process_url(value):
    value = strconv(value)
    u = urlparse(value)
    if u.scheme == "":
        url = "file://" + os.path.abspath(value)
    elif u.scheme == "file" and u.netloc != "":
        url = "file://" + os.path.abspath(os.path.join(u.netloc, u.path))
    else:
        url = value
    return url


def lookup_provider(scheme, region):
    lookup = {
        "gs": {
            "*": Provider.GOOGLE_STORAGE
        },
        "s3": {
            "us-east-1": Provider.S3,
            "us-east-2": Provider.S3_US_EAST2,
            "us-west-1": Provider.S3_US_WEST,
            "us-west-2": Provider.S3_US_WEST_OREGON
        },
        "swift": {
            "*": Provider.OPENSTACK_SWIFT
        }
    }

    if scheme in ["gs", "swift"]:
        region = "*"

    try:
        provider = lookup[scheme][region]
    except Exception:
        raise RuntimeError(
            "%s provider for region %s could not be found" % (scheme, region)
        )
    return provider


def lookup_credentials(scheme):
    lookup = {
        "gs": "~/.config/gcloud/application_default_credentials.json",
        "s3": "~/.aws/credentials",
    }

    key = None
    secret = None

    try:
        if scheme == "swift":
            key = os.environ["OS_USERNAME"]
            secret = os.environ["OS_PASSWORD"]
            if key is None or secret is None:
                raise

        elif scheme in lookup:
            fh = open(os.path.expanduser(lookup[scheme]), "r")
            content = fh.read()
            fh.close()
            if scheme == "gs":
                cred = json.loads(content)
                key = cred["client_id"]
                secret = key = cred["client_secret"]

            elif scheme == "s3":
                key = re.findall(
                    '(aws_access_key_id\ =\ )(.*)\n?', content
                )[0][1]
                secret = re.findall(
                    '(aws_secret_access_key\ =\ )(.*)\n?', content
                )[0][1]
    except Exception:
        raise RuntimeError(
            "%s credentials could not be set automatically, " % (scheme) +
            "please provide your key and secret"
        )

    return key, secret


def lookup_region(scheme):
    lookup = {
        "s3": "~/.aws/config",
    }

    region = None
    # return none if scheme isn't 's3'
    if scheme not in lookup:
        return region

    try:
        fh = open(os.path.expanduser(lookup[scheme]), "r")
        if scheme == "s3":
            content = fh.read()
            region = re.findall(
                '(region\ =\ )(.*)\n?', content
            )[0][1]
        fh.close()
    except Exception:
        raise RuntimeError(
            "%s region could not be set automatically, " % (scheme) +
            "please provide the region to use for your bucket"
        )
    return region


def lookup_project(scheme):
    lookup = {
        "gs": "~/.config/gcloud/configurations/config_default",
    }

    project = None
    # return none if scheme isn't 'gs'
    if scheme not in lookup:
        return project

    try:
        fh = open(os.path.expanduser(lookup[scheme]), "r")
        if scheme == "gs":
            content = fh.read()
            project = re.findall(
                '(project\ =\ )(.*)\n?', content
            )[0][1]
        fh.close()
    except Exception:
        raise RuntimeError(
            "%s project could not be set automatically, " % (scheme) +
            "please provide the project to use for your bucket"
        )
    return project
