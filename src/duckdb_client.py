"""
DuckDB manager for loading SAS files and exporting to CSV.
Uses DuckDB's read_stat extension to read SAS files from local temp files.
"""

import logging
import os
from collections import OrderedDict

import duckdb
import paramiko
from keboola.component.dao import BaseType, ColumnDefinition, SupportedDataTypes
from keboola.component.exceptions import UserException

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
        Initialize DuckDB client.

        Args:
            max_memory_mb: Maximum memory allocation in MB
        """
        self.max_memory_mb = max_memory_mb
        self.conn: duckdb.DuckDBPyConnection | None = None
        self._initialized = False

    def initialize(self):
        """
        Initialize DuckDB connection and install read_stat extension.

        Raises:
            UserException: If initialization fails
        """
        try:
            # Create temp directory
            os.makedirs(DUCKDB_DIR, exist_ok=True)

            # DuckDB configuration
            config = {
                "temp_directory": DUCKDB_DIR,
                "extension_directory": os.path.join(DUCKDB_DIR, "extensions"),
                "max_memory": f"{self.max_memory_mb}MB",
            }

            # Connect to DuckDB
            db_path = f"{DUCKDB_DIR}/sas_extractor.db"
            logging.info(f"Initializing DuckDB at {db_path}")
            self.conn = duckdb.connect(database=db_path, config=config)

            # Disable insertion order preservation for better performance
            self.conn.execute("SET preserve_insertion_order = false;")

            # Install and load read_stat extension from community repository
            logging.info("Installing DuckDB read_stat extension")
            self.conn.execute("INSTALL read_stat FROM community")
            self.conn.execute("LOAD read_stat")
            logging.info("DuckDB read_stat extension loaded successfully")

            self._initialized = True

        except Exception as e:
            raise UserException(f"Failed to initialize DuckDB: {e}")

    def load_sas_file(
        self,
        sftp_url: str,
        table_name: str,
        sftp_client: paramiko.SFTPClient,
    ) -> int:
        """
        Load SAS file from SFTP into DuckDB table.

        Downloads file via paramiko SFTP first, then reads with read_stat.

        Args:
            sftp_url: SFTP URL to SAS file (e.g., 'sftp:///path/to/file.sas7bdat')
            table_name: Name for DuckDB table
            sftp_client: Paramiko SFTP client for downloading file

        Returns:
            Number of rows loaded

        Raises:
            UserException: If loading fails
        """
        if not self._initialized or self.conn is None:
            raise UserException("DuckDB not initialized. Call initialize() first.")

        temp_file = None

        try:
            logging.info(f"Loading SAS file from {sftp_url} into table '{table_name}'")

            # Create temp file in DuckDB directory
            temp_file = os.path.join(DUCKDB_DIR, f"temp_{table_name}.sas7bdat")

            # Extract path from sftp:// URL
            remote_path = sftp_url.replace("sftp://", "")

            # Check SFTP client is still connected
            try:
                sftp_client.listdir(".")
            except Exception as e:
                raise UserException(f"SFTP connection lost before download: {e}")

            # Download file using paramiko SFTP client
            logging.info(f"Starting download from SFTP to local staging: {remote_path}")
            try:
                sftp_client.get(remote_path, temp_file)
            except PermissionError:
                raise UserException(f"Permission denied downloading {remote_path}. Check SFTP user permissions.")
            except Exception as e:
                raise UserException(f"Failed to download file from SFTP: {e}")

            logging.info(f"File successfully staged to {temp_file}")

            # Now read_stat can read the local file
            self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")

            # Build CREATE TABLE query using local file
            query = f"CREATE TABLE {table_name} AS SELECT * FROM read_stat('{temp_file}', format = 'sas7bdat')"

            logging.debug(f"Executing query: {query}")
            self.conn.execute(query)

            # Get row count
            result = self.conn.execute(f"SELECT COUNT(*) as count FROM {table_name}").fetchone()
            row_count = result[0] if result else 0

            logging.info(f"Successfully loaded {row_count:,} rows into '{table_name}'")
            return row_count

        except duckdb.IOException as e:
            raise UserException(f"Failed to read SAS file: {e}")
        except duckdb.Error as e:
            raise UserException(f"DuckDB error loading SAS file: {e}")
        except Exception as e:
            raise UserException(f"Unexpected error loading SAS file: {e}")
        finally:
            # Clean up temp file
            if temp_file and os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                    logging.debug(f"Removed temp file: {temp_file}")
                except Exception as e:
                    logging.warning(f"Failed to remove temp file: {e}")

    def get_table_schema(self, table_name: str) -> OrderedDict:
        """
        Get schema for a DuckDB table, mapped to Keboola types.

        Args:
            table_name: Name of DuckDB table

        Returns:
            OrderedDict mapping column names to ColumnDefinition objects
        """
        if not self._initialized or self.conn is None:
            raise UserException("DuckDB not initialized. Call initialize() first.")

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
        if not self._initialized or self.conn is None:
            raise UserException("DuckDB not initialized. Call initialize() first.")

        try:
            query = f"""
                COPY {table_name} TO '{output_path}'
                (HEADER, DELIMITER ',', FORCE_QUOTE *)
            """
            self.conn.execute(query)
            logging.info(f"Successfully exported '{table_name}' to CSV")

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
                logging.debug("DuckDB connection closed")
            except Exception as e:
                logging.warning(f"Error closing DuckDB connection: {e}")

        self._initialized = False
