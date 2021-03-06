import io
import os
import tempfile
import unittest

from tesseract.filestore import FileStore, FileExistsError


class TestFileStore(unittest.TestCase):
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

    def test_unspported(self):
        with self.assertRaises(ValueError):
            FileStore("fake://foo/bar")

    def test_init(self):
        self.assertTrue(os.path.exists(self.fs_path))
        self.assertEqual(self.fs.scheme, "file")
        self.assertEqual(self.fs.bucket, "")
        self.assertEqual(self.fs.path, self.fs_path)
        self.assertEqual(self.fs.key, None)
        self.assertEqual(self.fs.secret, None)

    def test_generate_url(self):
        self.assertEqual(
            self.fs.generate_url("1/test"),
            "%s://%s" % (
                self.fs.scheme,
                os.path.join(self.fs.bucket,
                             self.fs.path,
                             "1/test")
            )
        )

    def test_exists(self):
        tmpd = os.path.join(self.fs_path, "testdir")
        os.mkdir(tmpd)
        tmpf = tempfile.NamedTemporaryFile(mode="w", dir=tmpd, delete=False)
        tmpf.close()
        self.assertTrue(
            self.fs.exists(
                os.path.join(
                    os.path.basename(tmpd),
                    os.path.basename(tmpf.name)
                ),
                type='f'
            )
        )
        self.assertTrue(
            self.fs.exists(os.path.basename(tmpd), type='d')
        )
        self.assertFalse(
            self.fs.exists("doesnotexist")
        )
        with self.assertRaises(ValueError):
            self.fs.exists(tmpd, type="faketype")

    def test_upload(self):
        u = self.fs.upload(
            name="testfile.txt",
            contents="hello",
            overwrite_existing=False
        )
        self.assertTrue(os.path.exists(u))
        self.assertEqual(io.open(u, "r").read(), "hello")

        # overwrite existing
        u = self.fs.upload(
            name="testfile.txt",
            contents="world",
            overwrite_existing=True
        )
        self.assertEqual(io.open(u, "r").read(), "world")

        # no overwrite
        with self.assertRaises(FileExistsError):
            self.fs.upload(
                name="testfile.txt", contents="hello", overwrite_existing=False
            )

        # mutually exclusive inputs
        with self.assertRaises(ValueError):
            self.fs.upload(path="/tmp/foo.txt", contents="hello")

        with self.assertRaises(ValueError):
            self.fs.upload()

    def test_download(self):
        src = tempfile.NamedTemporaryFile(
            mode="w", dir=self.fs_path, delete=False
        )
        src.write("content")
        src.close()
        src2 = tempfile.NamedTemporaryFile(
            mode="w", dir=self.fs_path, delete=False
        )
        src2.write("other content")
        src2.close()
        dest = os.path.join(self.tmpdir, "test_dest.txt")

        p = self.fs.download(
            os.path.basename(src.name), dest, overwrite_existing=False
        )
        self.assertTrue(os.path.exists(p))
        self.assertEqual(open(p, 'r').read(), "content")

        # overwrite existing
        p = self.fs.download(
            os.path.basename(src2.name), dest, overwrite_existing=True
        )
        self.assertEqual(open(p, 'r').read(), "other content")

        # no overwrite
        with self.assertRaises(FileExistsError):
            self.fs.download(
                os.path.basename(src.name), dest, overwrite_existing=False
            )
