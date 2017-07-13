from __future__ import absolute_import, print_function

import os
import tes

PACKAGE_DIR = os.path.dirname(__file__)
RUNNER = os.path.join(PACKAGE_DIR, "resources", "runner.py")


def _create_task(file_store, cp_runner, input_files, docker,
                 cpu_cores, ram_gb, disk_gb, libraries):

    to_install = ["cloudpickle"] + libraries

    if docker is None:
        docker = "python:2.7"

    cmd_install_reqs = "pip install %s" % (" ".join(to_install))
    cmd_tesla = "python /tmp/tesla.py /tmp/tesla_func.pickle"
    cmd = cmd_install_reqs + " && " + cmd_tesla

    task = tes.Task(
        name="tesla remote execution",
        inputs=[
            tes.TaskParameter(
                name="tesla runner script",
                path="/tmp/tesla.py",
                type="FILE",
                contents=open(RUNNER, "r").read()
            ),
            tes.TaskParameter(
                name="pickled func",
                path="/tmp/tesla_func.pickle",
                type="FILE",
                contents=cp_runner
            )
        ],
        outputs=[
            tes.TaskParameter(
                name="pickled result",
                url=file_store.path + "/" + "tesla_result.pickle",
                path="/tmp/tesla_result.pickle",
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
                stdout="stdout",
                stderr="stderr"
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
