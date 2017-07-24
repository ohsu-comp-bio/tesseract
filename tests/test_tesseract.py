import os
import tempfile
import tes
import unittest

from tesseract.filestore import FileStore
from tesseract.tesseract import Tesseract


class TestTesseract(unittest.TestCase):

    testdir = os.path.dirname(
        os.path.realpath(__file__)
    )

    if not os.path.exists(os.path.join(testdir, "test_tmp")):
        os.mkdir(os.path.join(testdir, "test_tmp"))

    tmpdir = tempfile.mkdtemp(
        dir=os.path.join(testdir, "test_tmp"),
        prefix="tmp"
    )

    fs_path = os.path.join(tmpdir, "filestore")
    fs = FileStore(fs_path)

    runner = Tesseract(fs, "http://localhost:8000")

    def test_init(self):
        self.assertEqual(self.runner.input_files, [])
        self.assertEqual(self.runner.output_files, [])
        self.assertEqual(self.runner.cpu_cores, None)
        self.assertEqual(self.runner.ram_gb, None)
        self.assertEqual(self.runner.disk_gb, None)
        self.assertEqual(self.runner.docker, "python:2.7")
        self.assertEqual(self.runner.libraries, ["cloudpickle"])

    def test_with_resources(self):
        r = self.runner.with_resources(cpu_cores=2, ram_gb=4)
        self.assertEqual(r.input_files, [])
        self.assertEqual(r.output_files, [])
        self.assertEqual(r.cpu_cores, 2)
        self.assertEqual(r.ram_gb, 4)
        self.assertEqual(r.disk_gb, None)
        self.assertEqual(r.docker, "python:2.7")
        self.assertEqual(r.libraries, ["cloudpickle"])

    def test_with_input(self):
        r = self.runner.with_resources()
        r.with_input("file://tmp/input", "/mnt/input")
        self.assertEqual(
            r.input_files,
            [
                tes.TaskParameter(
                    url="file://tmp/input",
                    path="/mnt/input"
                )
            ]
        )

        with self.assertRaises(ValueError):
            self.runner.with_input("fake://tmp/input", "/mnt/input")

        with self.assertRaises(ValueError):
            self.runner.with_input("file://tmp/input", "../input")
