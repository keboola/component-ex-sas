"""
Converter for SAS files to CSV format.
Uses pyreadstat with optimized chunked streaming for memory-efficient processing of large files.
"""

import csv
import logging
import os
import time
from collections import OrderedDict

import duckdb
import pyreadstat
from keboola.component.dao import BaseType, ColumnDefinition, SupportedDataTypes
from keboola.component.exceptions import UserException

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

    def __init__(self, max_memory_mb: int, batch_size: int = 100000):
        """
        Initialize converter and DuckDB connection.

        Args:
            max_memory_mb: Maximum memory allocation in MB
            batch_size: Number of rows to process at once (default: 100k for optimal performance)

        Raises:
            UserException: If initialization fails
        """
        self.max_memory_mb = max_memory_mb
        self.batch_size = batch_size

        try:
            # Create temp directory
            os.makedirs(TEMP_DIR, exist_ok=True)

            # DuckDB configuration (for schema detection)
            config = {
                "temp_directory": TEMP_DIR,
                "max_memory": f"{self.max_memory_mb}MB",
            }

            self.conn = duckdb.connect(config=config)

        except Exception as e:
            raise UserException(f"Failed to initialize converter: {e}")

    def load_sas_file_and_convert_to_csv(
        self,
        sftp_url: str,
        table_name: str,
        output_path: str,
        sftp_client: SftpClient,
    ) -> tuple[int, dict]:
        """
        Stream SAS file from SFTP directly to CSV using optimized pyreadstat chunking.

        Workflow:
        1. Download SAS file via SFTP to temp directory
        2. Detect schema using DuckDB (first 1000 rows)
        3. Convert SAS to CSV using pyreadstat with optimized chunk processing

        Args:
            sftp_url: SFTP URL to SAS file
            table_name: Name for the table (used for logging)
            output_path: Path where CSV should be written (from table definition)
            sftp_client: SftpClient instance

        Returns:
            Tuple of (row_count, schema_dict)

        Raises:
            UserException: If loading fails
        """
        temp_file = os.path.join(TEMP_DIR, f"temp_{table_name}.sas7bdat")

        try:
            # Step 1: Download SAS file from SFTP
            logging.info(f"Downloading SAS file from {sftp_url}")
            remote_path = sftp_url.replace("sftp://", "")

            start_dl = time.time()
            sftp_client.download_file(remote_path, temp_file)
            logging.info(f"Downloaded in {time.time() - start_dl:.2f} seconds")

            # Step 2: Detect schema using DuckDB (efficient, reads minimal data)
            logging.info("Detecting schema...")
            schema_dict = self._detect_schema_with_duckdb(temp_file)
            logging.info(f"Schema detected: {len(schema_dict)} columns")

            # Step 3: Convert SAS to CSV using optimized pyreadstat
            start_convert = time.time()
            total_rows = self._convert_sas_to_csv_optimized(temp_file, output_path)

            total_duration = time.time() - start_convert
            logging.info(
                f"Converted {total_rows:,} rows to CSV in {total_duration:.2f} seconds "
                f"({total_rows / total_duration:.0f} rows/sec)"
            )

            return total_rows, schema_dict

        except Exception as e:
            raise UserException(f"Failed to process SAS file: {e}")
        finally:
            # Clean up temp file
            if os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                except Exception as e:
                    logging.warning(f"Failed to remove temp file: {e}")

    def _detect_schema_with_duckdb(self, sas_file_path: str) -> dict:
        """
        Detect schema from SAS file using pyreadstat metadata and map to DuckDB types.

        Args:
            sas_file_path: Path to SAS file

        Returns:
            Dictionary mapping column names to type strings
        """
        try:
            # Read first small chunk to get actual data types
            df, meta = pyreadstat.read_sas7bdat(
                sas_file_path,
                row_limit=1000,  # Only read first 1000 rows for schema
                disable_datetime_conversion=False,  # Keep types for detection
            )

            # Map pandas dtypes to type strings
            schema = {}
            for col_name in df.columns:
                pandas_dtype = str(df[col_name].dtype)
                schema[col_name] = pandas_dtype

            return schema

        except Exception as e:
            # Ultimate fallback: metadata only
            logging.warning(f"Schema detection failed, using metadata only: {e}")
            _, meta = pyreadstat.read_sas7bdat(sas_file_path, metadataonly=True)

            schema = {}
            for col_name in meta.column_names:
                schema[col_name] = "object"  # Default to string

            return schema

    def _convert_sas_to_csv_optimized(self, sas_file_path: str, csv_file_path: str) -> int:
        """
        Convert SAS file to CSV using optimized pyreadstat chunking.

        Optimizations:
        - Large chunk size (100k rows)
        - Disable datetime conversion
        - Large write buffer (8MB)
        - Batch row writes

        Args:
            sas_file_path: Path to input SAS file
            csv_file_path: Path to output CSV file

        Returns:
            Total number of rows converted
        """
        logging.info(f"Converting SAS to CSV with chunk size {self.batch_size:,}")

        # Create iterator for chunked reading
        reader = pyreadstat.read_file_in_chunks(
            pyreadstat.read_sas7bdat,
            sas_file_path,
            chunksize=self.batch_size,
            disable_datetime_conversion=True,  # Performance optimization
        )

        total_rows = 0
        chunk_num = 0

        # Open CSV with large buffer for better I/O performance
        with open(csv_file_path, "w", newline="", encoding="utf-8", buffering=8 * 1024 * 1024) as csvfile:
            csv_writer = None

            for df, meta in reader:
                chunk_num += 1
                chunk_start = time.time()

                # Initialize CSV writer on first chunk
                if csv_writer is None:
                    csv_writer = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)
                    # Write header
                    csv_writer.writerow(df.columns.tolist())

                # Batch write rows (more efficient than row-by-row)
                csv_writer.writerows(df.itertuples(index=False, name=None))

                rows_in_chunk = len(df)
                total_rows += rows_in_chunk

                chunk_duration = time.time() - chunk_start
                logging.info(
                    f"Chunk {chunk_num}: {rows_in_chunk:,} rows (total: {total_rows:,}) in {chunk_duration:.2f}s"
                )

        return total_rows

    def convert_schema_to_keboola(self, schema_dict: dict) -> OrderedDict:
        """
        Convert schema dictionary to Keboola schema format.

        Args:
            schema_dict: Schema dictionary (column_name -> type_string)

        Returns:
            OrderedDict mapping column names to ColumnDefinition objects
        """
        schema = OrderedDict()
        for col_name, type_string in schema_dict.items():
            schema[col_name] = ColumnDefinition(
                data_types=BaseType(dtype=self._convert_type_to_keboola(str(type_string))),
                primary_key=False,
            )

        return schema

    @staticmethod
    def _convert_type_to_keboola(type_string: str) -> SupportedDataTypes:
        """Map pandas/data types to Keboola data types."""
        dtype = type_string.lower()

        if any(
            x in dtype for x in ["int", "integer", "tinyint", "smallint", "bigint", "int8", "int16", "int32", "int64"]
        ):
            return SupportedDataTypes.INTEGER
        elif any(x in dtype for x in ["decimal", "numeric"]):
            return SupportedDataTypes.NUMERIC
        elif any(x in dtype for x in ["float", "double", "real", "float32", "float64"]):
            return SupportedDataTypes.FLOAT
        elif "bool" in dtype:
            return SupportedDataTypes.BOOLEAN
        elif "datetime" in dtype or "timestamp" in dtype:
            return SupportedDataTypes.TIMESTAMP
        elif "date" in dtype:
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
