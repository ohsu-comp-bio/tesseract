from __future__ import absolute_import, print_function

import errno
import os
import tes

from urlparse import urlparse


PACKAGE_DIR = os.path.dirname(__file__)
RUNNER = os.path.join(PACKAGE_DIR, "resources", "runner.py")


def _create_task(input_cp_url, output_cp_url, input_files,
                 docker, cpu_cores, ram_gb, disk_gb, libraries):

    runner_path = "/tmp/tesseract.py"
    input_cp_path = "/tmp/tesseract_func.pickle"

    to_install = ["cloudpickle"] + libraries
    cmd_install_reqs = "pip install %s" % (" ".join(to_install))
    cmd_tesseract = "python %s %s" % (runner_path, input_cp_path)

    if docker is None:
        docker = "python:2.7"
        cmd = cmd_install_reqs + " && " + cmd_tesseract
    else:
        cmd = cmd_tesseract

    task = tes.Task(
        name="tesseract remote execution",
        inputs=[
            tes.TaskParameter(
                name="tesseract runner script",
                path=runner_path,
                type="FILE",
                contents=open(RUNNER, "r").read()
            ),
            tes.TaskParameter(
                name="pickled runner",
                url=input_cp_url,
                path=input_cp_path,
                type="FILE"
            )
        ],
        outputs=[
            tes.TaskParameter(
                name="pickled result",
                url=output_cp_url,
                path="/tmp/tesseract_result.pickle",
                type="FILE"
            )
        ],
        resources=tes.Resources(
            cpu_cores=cpu_cores,
            ram_gb=ram_gb,
            size_gb=disk_gb
        ),
        executors=[
            tes.Executor(
                image_name=docker,
                cmd=["sh", "-c", cmd],
                stdout="/tmp/stdout",
                stderr="/tmp/stderr"
            )
        ]
    )

    for v in input_files:
        task.inputs.append(
            tes.TaskParameter(
                url=v.url,
                path=v.path,
                type="FILE"
            )
        )

    return task


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
