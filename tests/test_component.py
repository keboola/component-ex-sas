"""
Unit tests for SAS File Extractor component.
"""

import os
import unittest
from unittest import mock

from keboola.component.dao import SupportedDataTypes
from keboola.component.exceptions import UserException

from configuration import Configuration, OutputSettings
from duckdb_client import DuckDBClient


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
            sas_tables=["customers.sas7bdat", "orders.sas7bdat"],
        )

        self.assertEqual(config.sftp.host, "sftp.example.com")
        self.assertEqual(len(config.sas_tables), 2)
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

    def test_invalid_sas_files(self):
        """Test that non-.sas7bdat files are rejected."""
        with self.assertRaises(UserException):
            Configuration(
                sftp={
                    "host": "sftp.example.com",
                    "username": "user",
                    "#password": "pass",
                    "folder_path": "/data",
                },
                sas_tables=["customers.csv"],  # Invalid extension
            )

    def test_empty_sas_files_list(self):
        """Test that empty sas_files list is rejected."""
        with self.assertRaises(UserException):
            Configuration(
                sftp={
                    "host": "sftp.example.com",
                    "username": "user",
                    "#password": "pass",
                    "folder_path": "/data",
                },
                sas_tables=[],  # Empty list
            )

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
            sas_tables=["customers.sas7bdat"],
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
            sas_tables=["customers.sas7bdat"],
        )

        self.assertEqual(config.get_table_name("customers.sas7bdat"), "customers")
        self.assertEqual(config.get_table_name("my_table.sas7bdat"), "my_table")


# StateManager tests removed - feature was removed from component


class TestDuckDBClient(unittest.TestCase):
    """Test DuckDB type conversion."""

    def test_convert_integer_types(self):
        """Test conversion of integer types."""
        self.assertEqual(
            DuckDBClient._convert_duckdb_type("INTEGER"),
            SupportedDataTypes.INTEGER,
        )
        self.assertEqual(
            DuckDBClient._convert_duckdb_type("BIGINT"),
            SupportedDataTypes.INTEGER,
        )
        self.assertEqual(
            DuckDBClient._convert_duckdb_type("SMALLINT"),
            SupportedDataTypes.INTEGER,
        )

    def test_convert_numeric_types(self):
        """Test conversion of numeric/decimal types."""
        self.assertEqual(
            DuckDBClient._convert_duckdb_type("DECIMAL"),
            SupportedDataTypes.NUMERIC,
        )
        self.assertEqual(
            DuckDBClient._convert_duckdb_type("NUMERIC"),
            SupportedDataTypes.NUMERIC,
        )

    def test_convert_float_types(self):
        """Test conversion of float types."""
        self.assertEqual(
            DuckDBClient._convert_duckdb_type("DOUBLE"),
            SupportedDataTypes.FLOAT,
        )
        self.assertEqual(
            DuckDBClient._convert_duckdb_type("FLOAT"),
            SupportedDataTypes.FLOAT,
        )

    def test_convert_boolean_type(self):
        """Test conversion of boolean type."""
        self.assertEqual(
            DuckDBClient._convert_duckdb_type("BOOLEAN"),
            SupportedDataTypes.BOOLEAN,
        )

    def test_convert_timestamp_types(self):
        """Test conversion of timestamp types."""
        self.assertEqual(
            DuckDBClient._convert_duckdb_type("TIMESTAMP"),
            SupportedDataTypes.TIMESTAMP,
        )
        self.assertEqual(
            DuckDBClient._convert_duckdb_type("TIMESTAMP WITH TIME ZONE"),
            SupportedDataTypes.TIMESTAMP,
        )

    def test_convert_date_type(self):
        """Test conversion of date type."""
        self.assertEqual(
            DuckDBClient._convert_duckdb_type("DATE"),
            SupportedDataTypes.DATE,
        )

    def test_convert_string_types(self):
        """Test conversion of string types."""
        self.assertEqual(
            DuckDBClient._convert_duckdb_type("VARCHAR"),
            SupportedDataTypes.STRING,
        )
        self.assertEqual(
            DuckDBClient._convert_duckdb_type("TEXT"),
            SupportedDataTypes.STRING,
        )
        self.assertEqual(
            DuckDBClient._convert_duckdb_type("UNKNOWN_TYPE"),
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
