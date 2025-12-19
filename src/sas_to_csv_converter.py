"""
Converter for SAS files to CSV format.
Uses Polars with polars_readstat to read SAS files, then DuckDB to export to CSV.
"""

import logging
import os
import time
from collections import OrderedDict

import duckdb
from keboola.component.dao import BaseType, ColumnDefinition, SupportedDataTypes
from keboola.component.exceptions import UserException
from polars_readstat import scan_readstat

from sftp_client import SftpClient

TEMP_DIR = os.path.join(os.environ.get("TMPDIR", "/tmp"), "sas_converter")


class SasToCsvConverter:
    """
    Converter for SAS files to CSV format.

    Handles:
    - Loading SAS files from SFTP using Polars + polars_readstat
    - Schema detection and type mapping
    - Exporting to CSV using DuckDB
    """

    def __init__(self, max_memory_mb: int):
        """
        Initialize converter and DuckDB connection.

        Args:
            max_memory_mb: Maximum memory allocation in MB

        Raises:
            UserException: If initialization fails
        """
        self.max_memory_mb = max_memory_mb

        try:
            # Create temp directory
            os.makedirs(TEMP_DIR, exist_ok=True)

            # DuckDB configuration
            config = {
                "temp_directory": TEMP_DIR,
                "max_memory": f"{self.max_memory_mb}MB",
            }

            self.conn = duckdb.connect(config=config)

            # Disable insertion order to prevent OOM
            self.conn.execute("SET preserve_insertion_order = false;")

        except Exception as e:
            raise UserException(f"Failed to initialize converter: {e}")

    def load_sas_file(
        self,
        sftp_url: str,
        table_name: str,
        sftp_client: SftpClient,
    ) -> int:
        """
        Load SAS file from SFTP into DuckDB table via Polars.

        Downloads file via SFTP, reads with Polars + polars_readstat, then registers with DuckDB.

        Args:
            sftp_url: SFTP URL to SAS file
            table_name: Name for DuckDB table
            sftp_client: SftpClient instance

        Returns:
            Number of rows loaded

        Raises:
            UserException: If loading fails
        """

        temp_file = os.path.join(TEMP_DIR, f"temp_{table_name}.sas7bdat")

        try:
            logging.info(f"Loading SAS file from {sftp_url} into table '{table_name}'")
            remote_path = sftp_url.replace("sftp://", "")

            start_dl = time.time()
            sftp_client.download_file(remote_path, temp_file)

            logging.info(f"File {table_name} downloaded to stage in {time.time() - start_dl:.2f} seconds")
            start_read = time.time()

            # table name is referenced in the query
            df = scan_readstat(temp_file)  # noqa: F841

            logging.info(f"SAS file {table_name} scanned in {time.time() - start_read:.2f} seconds")

            self.conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")

            result = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            row_count = result[0] if result else 0

            return row_count

        except Exception as e:
            raise UserException(f"Failed to load SAS file: {e}")

    def get_table_schema(self, table_name: str) -> OrderedDict:
        """
        Get schema for a DuckDB table, mapped to Keboola types.

        Args:
            table_name: Name of DuckDB table

        Returns:
            OrderedDict mapping column names to ColumnDefinition objects
        """

        try:
            # Get table metadata
            table_meta = self.conn.execute(f"DESCRIBE {table_name}").fetchall()

            schema = OrderedDict()
            for column in table_meta:
                col_name = column[0]
                col_type = column[1]

                schema[col_name] = ColumnDefinition(
                    data_types=BaseType(dtype=self._convert_duckdb_type(col_type)),
                    primary_key=False,
                )

            return schema

        except Exception as e:
            raise UserException(f"Failed to get schema for table {table_name}: {e}")

    def export_to_csv(self, table_name: str, output_path: str):
        """
        Export DuckDB table to CSV file.
        """

        try:
            start_export = time.time()
            query = f"COPY {table_name} TO '{output_path}' (HEADER, DELIMITER ',', FORCE_QUOTE *)"
            self.conn.execute(query)
            export_duration = time.time() - start_export

            logging.info(f"Table {table_name} written to CSV in {export_duration:.2f} seconds")

        except Exception as e:
            raise UserException(f"Failed to export table to CSV: {e}")

    @staticmethod
    def _convert_duckdb_type(duckdb_type: str) -> SupportedDataTypes:
        # Normalize type to uppercase
        dtype = duckdb_type.upper()

        if dtype in ["TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT"]:
            return SupportedDataTypes.INTEGER
        elif dtype in ["REAL", "DECIMAL", "NUMERIC"]:
            return SupportedDataTypes.NUMERIC
        elif dtype in ["DOUBLE", "FLOAT"]:
            return SupportedDataTypes.FLOAT
        elif dtype == "BOOLEAN":
            return SupportedDataTypes.BOOLEAN
        elif "TIMESTAMP" in dtype:
            return SupportedDataTypes.TIMESTAMP
        elif dtype == "DATE":
            return SupportedDataTypes.DATE
        else:
            return SupportedDataTypes.STRING

    def close(self):
        """Close DuckDB connection and clean up resources."""
        if self.conn:
            try:
                self.conn.close()
            except Exception as e:
                logging.warning(f"Error closing DuckDB connection: {e}")
