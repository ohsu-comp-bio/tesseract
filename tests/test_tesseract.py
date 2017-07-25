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

    def test_clone(self):
        r = self.runner.clone()
        self.assertEqual(r, self.runner)
        r.cpu_cores = 1
        self.assertNotEqual(r, self.runner)

    def test_with_resources(self):
        r = self.runner.clone()
        r.with_resources(cpu_cores=2, ram_gb=4)
        self.assertEqual(r.input_files, [])
        self.assertEqual(r.output_files, [])
        self.assertEqual(r.cpu_cores, 2)
        self.assertEqual(r.ram_gb, 4)
        self.assertEqual(r.disk_gb, None)
        self.assertEqual(r.docker, "python:2.7")
        self.assertEqual(r.libraries, ["cloudpickle"])

    def test_with_input(self):
        r = self.runner.clone()
        r.with_input("file:///tmp/input", "/mnt/input")
        r.with_input("file:///tmp/input2", "./input2")
        expected = [
            tes.TaskParameter(
                url="file:///tmp/input",
                path="/mnt/input"
            ),
            tes.TaskParameter(
                url="file:///tmp/input2",
                path="/tmp/tesseract/input2"
            )
        ]
        print("ACTUAL", r.input_files)
        print("EXPECTED", expected)
        self.assertEqual(
            r.input_files,
            expected
        )

        with self.assertRaises(ValueError):
            self.runner.with_input("fake://tmp/input", "/mnt/input")

        with self.assertRaises(ValueError):
            self.runner.with_input("file://tmp/input", "../input")

    def test_with_output(self):
        r = self.runner.clone()
        r._Tesseract__id = "testid"
        r.with_output("./output.txt")
        r.with_output("/mnt/output.txt")
        expected = [
            tes.TaskParameter(
                path="/tmp/tesseract/output.txt",
                url=r.file_store.generate_url("testid/output.txt"),
                type="FILE"
            ),
            tes.TaskParameter(
                path="/mnt/output.txt",
                url=r.file_store.generate_url("testid/mnt/output.txt"),
                type="FILE"
            )
        ]
        print("ACTUAL", r.output_files)
        print("EXPECTED", expected)
        self.assertEqual(
            r.output_files,
            expected
        )

        with self.assertRaises(ValueError):
            self.runner.with_output("../output")
