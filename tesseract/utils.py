from __future__ import absolute_import, print_function

import errno
import os

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
