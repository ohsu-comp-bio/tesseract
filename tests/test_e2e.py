import os
import tempfile

from requests.utils import urlparse

from tesseract.filestore import FileStore
from tesseract.tesseract import Tesseract

from funnel_test_util import SimpleServerTest


class TestTesseractE2E(SimpleServerTest):

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

        r = self.runner.clone()
        f = r.run(hello, "world")
        self.assertEqual(f.result(), "hello world")

    def test_with_input_and_output(self):
        def cat(in_file, out_file):
            with open(in_file, "rb") as fh:
                content = fh.read()
            with open(out_file, "wb") as fh:
                fh.write(content)
            return True

        r = self.runner.clone()
        tmp_in = tempfile.NamedTemporaryFile(dir=self.tmpdir, delete=False)
        tmp_in.write("fizzbuzz")
        tmp_in.close()
        tmp_out = "./test_output.txt"
        r.with_input(tmp_in.name, tmp_in.name)
        r.with_output(tmp_out)
        f = r.run(cat, tmp_in.name, tmp_out)
        output_url = urlparse(r.output_files[0].url).path
        self.assertTrue(f.result())
        self.assertTrue(os.path.exists(output_url))
        self.assertEqual(
            open(output_url, "rb").read(),
            "fizzbuzz"
        )
