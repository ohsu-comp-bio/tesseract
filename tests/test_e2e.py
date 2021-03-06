import io
import os
import tempfile

from requests.utils import urlparse

from tesseract.filestore import FileStore
from tesseract.tesseract import Tesseract, Future, CachedFuture

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
            with io.open(in_file, "rb") as fh:
                content = fh.read()
            with io.open(out_file, "wb") as fh:
                fh.write(content)
            return True

        r = self.runner.clone()
        tmp_in = tempfile.NamedTemporaryFile(
            mode="w", dir=self.tmpdir, delete=False
        )
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
            io.open(output_url, "r").read(),
            "fizzbuzz"
        )

    def test_with_upload(self):
        def cat(in_file):
            with io.open(in_file, "rb") as fh:
                fh.read()
            return True

        r = self.runner.clone()
        r._Tesseract__id = "test-id"

        tmp_in = tempfile.NamedTemporaryFile(
            mode="w", dir=self.tmpdir, delete=False
        )
        tmp_in.write("fizzbuzz")
        tmp_in.close()

        r.with_upload(tmp_in.name)
        # this second call should do nothing since the fiel will already
        # have been uploaded
        r.with_upload(tmp_in.name)

        f = r.run(cat, tmp_in.name)
        self.assertTrue(f.result())
        self.assertTrue(
            self.fs.exists(
                os.path.join("test-id", os.path.basename(tmp_in.name))
            )
        )

    def test_with_call_caching(self):
        def hello(s):
            return "hello %s" % (s)

        r = self.runner.clone()
        r.with_call_caching("test_cache")

        f = r.run(hello, "world")
        self.assertEqual(f.result(), "hello world")
        self.assertTrue(isinstance(f, Future))

        cf = r.run(hello, "world")
        self.assertEqual(cf.result(), "hello world")
        self.assertTrue(isinstance(cf, CachedFuture))
