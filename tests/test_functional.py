import unittest

from keboola.datadirtest import DataDirTester


class TestFunctional(unittest.TestCase):
    def test_functional_sftp(self):
        DataDirTester(data_dir="./tests/test_functional_sftp").run()


if __name__ == "__main__":
    unittest.main()
