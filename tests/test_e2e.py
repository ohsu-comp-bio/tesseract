import os
import tempfile

from tesseract.filestore import FileStore
from tesseract.tesseract import Tesseract

from funnel_test_util import SimpleServerTest


class TestTesseract(SimpleServerTest):
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

    def test_hello_world(self):
        def hello(s):
            return "hello %s" % (s)

        f = self.runner.run(hello, "world")
        self.assertEqual(f.result(), "hello world")
