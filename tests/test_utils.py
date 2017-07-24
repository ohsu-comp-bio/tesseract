import os
import tempfile
import unittest

from libcloud.storage.types import Provider

from tesseract.utils import (makedirs, process_url, lookup_provider,
                             lookup_region, lookup_project)


class TestUtils(unittest.TestCase):
    testdir = os.path.dirname(
        os.path.realpath(__file__)
    )

    if not os.path.exists(os.path.join(testdir, "test_tmp")):
        os.mkdir(os.path.join(testdir, "test_tmp"))

    tmpdir = tempfile.mkdtemp(
        dir=os.path.join(testdir, "test_tmp"),
        prefix="tmp"
    )

    def test_makedirs(self):
        p = os.path.join(self.tmpdir, "test/dirA")
        makedirs(p, exists_ok=True)
        self.assertTrue(os.path.exists(p))

        with self.assertRaises(OSError):
            makedirs(p, exists_ok=False)

    def test_proccess_url(self):
        self.assertEqual(
            process_url(os.path.join(self.tmpdir, "..")),
            "file://" + self.testdir + "/test_tmp"
        )

    def test_lookup_provider(self):
        self.assertEqual(
            Provider.S3_US_WEST_OREGON,
            lookup_provider("s3", "us-west-2")
        )

        self.assertEqual(
            lookup_provider("gs", "us-east-1"),
            lookup_provider("gs", "us-west-2")
        )

        self.assertEqual(
            lookup_provider("swift", "us-east-1"),
            lookup_provider("swift", "us-west-2")
        )

        with self.assertRaises(RuntimeError):
            lookup_provider("s3", "fake")

    def test_lookup_region(self):
        self.assertEqual(
            lookup_region("gs"),
            None
        )

        self.assertEqual(
            lookup_region("swift"),
            None
        )

    def test_lookup_project(self):
        self.assertEqual(
            lookup_project("s3"),
            None
        )

        self.assertEqual(
            lookup_project("swift"),
            None
        )
