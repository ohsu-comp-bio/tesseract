from __future__ import absolute_import, print_function

import errno
import json
import os
import re

from libcloud.storage.types import Provider
from urlparse import urlparse


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
    u = urlparse(value)
    if u.scheme == "":
        return "file://" + os.path.abspath(value)
    elif u.scheme == "file" and u.netloc != "":
        p = os.path.abspath(os.path.join(u.netloc, u.path))
        return "file://" + p
    else:
        return value


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
    return lookup[scheme][region]


def lookup_credentials(scheme):
    lookup = {
        "gs": "~/.config/gcloud/application_default_credentials.json",
        "s3": "~/.aws/credentials",
        "swift": None
    }

    key = None
    secret = None
    try:
        with open(lookup[scheme], "r") as fh:
            content = fh.read()
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
            else:
                pass
    except:
        raise RuntimeError(
            "%s credentials could not be set automatically, \
            please provide your key and secret" % (scheme)
        )
    return key, secret


def lookup_region(scheme):
    lookup = {
        "gs": None,
        "s3": "~/.aws/config",
        "swift": None
    }

    region = None
    try:
        with open(lookup[scheme], "r") as fh:
            content = fh.read()
            if scheme == "s3":
                region = re.findall(
                    '(region\ =\ )(.*)\n?', content
                )[0][1]
            else:
                pass
    except:
        raise RuntimeError(
            "%s region could not be set automatically, \
            please provide the region to use for your bucket" % (scheme)
        )
    return region
