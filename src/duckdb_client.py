"""
DuckDB manager for loading SAS files and exporting to CSV.
Uses DuckDB's read_stat extension to read SAS files from local temp files.
"""

import logging
import os
import time
from collections import OrderedDict

import duckdb
from keboola.component.dao import BaseType, ColumnDefinition, SupportedDataTypes
from keboola.component.exceptions import UserException

from sftp_client import SftpClient

DUCKDB_DIR = os.path.join(os.environ.get("TMPDIR", "/tmp"), "duckdb")


class DuckDBClient:
    """
    DuckDB client for SAS file processing.

    Handles:
    - DuckDB initialization with read_stat extension
    - Loading SAS files from SFTP into DuckDB tables
    - Schema detection and type mapping
    - Exporting DuckDB tables to CSV
    """

    def __init__(self, max_memory_mb: int):
        """
        Initialize DuckDB client and connection.

        Args:
            max_memory_mb: Maximum memory allocation in MB

        Raises:
            UserException: If initialization fails
        """
        self.max_memory_mb = max_memory_mb

        try:
            # Create temp directory
            os.makedirs(DUCKDB_DIR, exist_ok=True)

            # DuckDB configuration
            config = {
                "temp_directory": DUCKDB_DIR,
                "extension_directory": os.path.join(DUCKDB_DIR, "extensions"),
                "max_memory": f"{self.max_memory_mb}MB",
            }

            self.conn = duckdb.connect(config=config)

            # Disable insertion order to prevent OOM
            self.conn.execute("SET preserve_insertion_order = false;")

            self.conn.execute("INSTALL read_stat FROM community")
            self.conn.execute("LOAD read_stat")

        except Exception as e:
            raise UserException(f"Failed to initialize DuckDB: {e}")

    def load_sas_file(
        self,
        sftp_url: str,
        table_name: str,
        sftp_client: SftpClient,
    ) -> int:
        """
        Load SAS file from SFTP into DuckDB.

        Downloads file via optimized SFTP first, then creates a DuckDB VIEW.

        Args:
            sftp_url: SFTP URL to SAS file
            table_name: Name for DuckDB view
            sftp_client: SftpClient instance

        Returns:
            Number of rows loaded

        Raises:
            UserException: If loading fails
        """

        temp_file = os.path.join(DUCKDB_DIR, f"temp_{table_name}.sas7bdat")

        try:
            logging.info(f"Loading SAS file from {sftp_url} into view '{table_name}'")
            remote_path = sftp_url.replace("sftp://", "")

            # Time the download
            start_dl = time.time()
            sftp_client.download_file(remote_path, temp_file)
            dl_duration = time.time() - start_dl
            logging.info(f"File {table_name} downloaded to stage in {dl_duration:.2f} seconds")

            query = (
                f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_stat('{temp_file}', format = 'sas7bdat')"
            )
            self.conn.execute(query)

            # Get row count
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
