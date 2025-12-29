"""
Unit tests for SAS File Extractor component.
"""

import os
import unittest
from unittest import mock

from keboola.component.dao import SupportedDataTypes
from keboola.component.exceptions import UserException

from configuration import Configuration, OutputSettings
from sas_to_csv_converter import SasToCsvConverter


class TestConfiguration(unittest.TestCase):
    """Test configuration validation."""

    def test_valid_configuration(self):
        """Test valid configuration."""
        config = Configuration(
            sftp={
                "host": "sftp.example.com",
                "port": 22,
                "username": "user",
                "#password": "pass",
                "folder_path": "/data",
            },
            table="customers.sas7bdat",
        )

        self.assertEqual(config.sftp.host, "sftp.example.com")
        self.assertEqual(config.table, "customers.sas7bdat")
        self.assertIsInstance(config.output, OutputSettings)

    def test_missing_required_fields(self):
        """Test that missing required fields raise UserException."""
        with self.assertRaises(UserException):
            Configuration(
                sftp={
                    "host": "sftp.example.com",
                    "username": "user",
                    # Missing password
                }
            )

    def test_optional_table(self):
        """Test that table parameter is optional (for sync actions)."""
        config = Configuration(
            sftp={
                "host": "sftp.example.com",
                "username": "user",
                "#password": "pass",
                "folder_path": "/data",
            },
            table=None,
        )
        self.assertIsNone(config.table)

    # Incremental settings feature removed - test removed

    def test_folder_path_trailing_slash_removed(self):
        """Test that trailing slash is removed from folder_path."""
        config = Configuration(
            sftp={
                "host": "sftp.example.com",
                "username": "user",
                "#password": "pass",
                "folder_path": "/data/sas/",  # Has trailing slash
            },
            table="customers.sas7bdat",
        )

        self.assertEqual(config.sftp.folder_path, "/data/sas")

    def test_get_table_name(self):
        """Test table name extraction from SAS filename."""
        config = Configuration(
            sftp={
                "host": "sftp.example.com",
                "username": "user",
                "#password": "pass",
                "folder_path": "/data",
            },
            table="customers.sas7bdat",
        )

        self.assertEqual(config.get_table_name("customers.sas7bdat"), "customers")
        self.assertEqual(config.get_table_name("my_table.sas7bdat"), "my_table")


# StateManager tests removed - feature was removed from component


class TestSasToCsvConverter(unittest.TestCase):
    """Test SAS to CSV converter type conversion."""

    def test_convert_integer_types(self):
        """Test conversion of integer types."""
        self.assertEqual(
            SasToCsvConverter._convert_type_to_keboola("INTEGER"),
            SupportedDataTypes.INTEGER,
        )
        self.assertEqual(
            SasToCsvConverter._convert_type_to_keboola("BIGINT"),
            SupportedDataTypes.INTEGER,
        )
        self.assertEqual(
            SasToCsvConverter._convert_type_to_keboola("SMALLINT"),
            SupportedDataTypes.INTEGER,
        )

    def test_convert_numeric_types(self):
        """Test conversion of numeric/decimal types."""
        self.assertEqual(
            SasToCsvConverter._convert_type_to_keboola("DECIMAL"),
            SupportedDataTypes.NUMERIC,
        )
        self.assertEqual(
            SasToCsvConverter._convert_type_to_keboola("NUMERIC"),
            SupportedDataTypes.NUMERIC,
        )

    def test_convert_float_types(self):
        """Test conversion of float types."""
        self.assertEqual(
            SasToCsvConverter._convert_type_to_keboola("DOUBLE"),
            SupportedDataTypes.FLOAT,
        )
        self.assertEqual(
            SasToCsvConverter._convert_type_to_keboola("FLOAT"),
            SupportedDataTypes.FLOAT,
        )

    def test_convert_boolean_type(self):
        """Test conversion of boolean type."""
        self.assertEqual(
            SasToCsvConverter._convert_type_to_keboola("BOOLEAN"),
            SupportedDataTypes.BOOLEAN,
        )

    def test_convert_timestamp_types(self):
        """Test conversion of timestamp types."""
        self.assertEqual(
            SasToCsvConverter._convert_type_to_keboola("TIMESTAMP"),
            SupportedDataTypes.TIMESTAMP,
        )
        self.assertEqual(
            SasToCsvConverter._convert_type_to_keboola("TIMESTAMP WITH TIME ZONE"),
            SupportedDataTypes.TIMESTAMP,
        )

    def test_convert_date_type(self):
        """Test conversion of date type."""
        self.assertEqual(
            SasToCsvConverter._convert_type_to_keboola("DATE"),
            SupportedDataTypes.DATE,
        )

    def test_convert_string_types(self):
        """Test conversion of string types."""
        self.assertEqual(
            SasToCsvConverter._convert_type_to_keboola("VARCHAR"),
            SupportedDataTypes.STRING,
        )
        self.assertEqual(
            SasToCsvConverter._convert_type_to_keboola("TEXT"),
            SupportedDataTypes.STRING,
        )
        self.assertEqual(
            SasToCsvConverter._convert_type_to_keboola("UNKNOWN_TYPE"),
            SupportedDataTypes.STRING,
        )


# FileState tests removed - feature was removed from component


class TestComponentConfiguration(unittest.TestCase):
    """Test component initialization with configuration."""

    @mock.patch.dict(os.environ, {"KBC_DATADIR": "./non-existing-dir"})
    def test_component_no_config_fails(self):
        """Test that component fails without configuration."""
        with self.assertRaises(ValueError):
            from component import Component

            Component()


if __name__ == "__main__":
    unittest.main()
