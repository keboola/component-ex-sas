"""
Converter for SAS files to CSV format.
Uses pyreadstat with Polars output for memory-efficient processing of large files.
"""

import logging
import os
import time
from collections import OrderedDict
from datetime import datetime, timedelta

import duckdb
import polars as pl
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

    def __init__(
        self,
        max_memory_mb: int,
        batch_size: int = 100000,
        null_values: list[str] | None = None,
        encoding: str | None = None,
        infer_dtypes: bool = True,
    ):
        """
        Initialize converter and DuckDB connection.

        Args:
            max_memory_mb: Maximum memory allocation in MB
            batch_size: Number of rows to process at once (default: 100k for optimal performance)
            null_values: List of strings to treat as NULL values
            encoding: Encoding override for SAS files (iconv-compatible name, e.g. 'CP1250' for WLATIN2)
            infer_dtypes: If True, infer types from data sample. If False, use SAS metadata types.

        Raises:
            UserException: If initialization fails
        """
        self.max_memory_mb = max_memory_mb
        self.batch_size = batch_size
        self.null_values = null_values or []
        self.encoding = encoding
        self.infer_dtypes = infer_dtypes

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

    def infer_sas_schema(
        self,
        sftp_url: str,
        table_name: str,
        sftp_client: SftpClient,
    ) -> tuple[str, dict]:
        """
        Download SAS file from SFTP and infer its schema.

        Workflow:
        1. Download SAS file via SFTP to temp directory
        2. Detect schema using DuckDB (first 1000 rows)

        Args:
            sftp_url: SFTP URL to SAS file
            table_name: Name for the table (used for logging)
            sftp_client: SftpClient instance

        Returns:
            Tuple of (temp_file_path, schema_dict)

        Raises:
            UserException: If download or schema detection fails
        """
        temp_file = os.path.join(TEMP_DIR, f"temp_{table_name}.sas7bdat")

        try:
            # Step 1: Download SAS file from SFTP
            logging.info(f"Downloading SAS file from {sftp_url}")
            remote_path = sftp_url.replace("sftp://", "")

            start_dl = time.time()
            sftp_client.download_file(remote_path, temp_file)
            logging.info(f"Downloaded in {time.time() - start_dl:.2f} seconds")

            # Step 2: Detect schema
            logging.info("Detecting schema...")
            if self.infer_dtypes:
                schema_dict = self._detect_schema_with_duckdb(temp_file)
            else:
                schema_dict = self._detect_schema_from_metadata(temp_file)
            logging.info(f"Schema detected: {len(schema_dict)} columns")

            return temp_file, schema_dict

        except Exception as e:
            # Clean up temp file on error
            if os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                except Exception as cleanup_error:
                    logging.warning(f"Failed to remove temp file: {cleanup_error}")
            raise UserException(f"Failed to infer schema from SAS file: {e}")

    def convert_sas_to_csv(
        self,
        temp_file: str,
        output_path: str,
        incremental_field: str | None = None,
        last_incremental_value: float | int | None = None,
    ) -> int:
        """
        Convert already downloaded SAS file to CSV using optimized pyreadstat chunking.

        Args:
            temp_file: Path to temporary SAS file (from infer_sas_schema)
            output_path: Path where CSV should be written (from table definition)
            incremental_field: Column name for incremental filtering (optional)
            last_incremental_value: Last incremental value to filter from (optional)

        Returns:
            Total number of rows converted

        Raises:
            UserException: If conversion fails
        """
        try:
            # Convert SAS to CSV using optimized pyreadstat
            start_convert = time.time()
            total_rows = self._convert_sas_to_csv_optimized(
                temp_file, output_path, incremental_field, last_incremental_value
            )

            total_duration = time.time() - start_convert
            logging.info(
                f"Converted {total_rows:,} rows to CSV in {total_duration:.2f} seconds "
                f"({total_rows / total_duration:.0f} rows/sec)"
            )

            return total_rows

        except Exception as e:
            raise UserException(f"Failed to convert SAS file to CSV: {e}")
        finally:
            # Clean up temp file
            if os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                except Exception as e:
                    logging.warning(f"Failed to remove temp file: {e}")

    def _detect_schema_with_duckdb(self, sas_file_path: str) -> dict:
        """
        Detect schema from SAS file using pyreadstat with Polars output.

        Args:
            sas_file_path: Path to SAS file

        Returns:
            Dictionary mapping column names to type strings
        """
        try:
            # Read first small chunk to get actual data types using Polars
            df, meta = pyreadstat.read_sas7bdat(
                sas_file_path,
                row_limit=1000,  # Only read first 1000 rows for schema
                disable_datetime_conversion=False,
                output_format="polars",
                encoding=self.encoding,
            )

            # Map Polars dtypes to type strings
            schema = {}
            for col_name in df.columns:
                polars_dtype = str(df[col_name].dtype)
                schema[col_name] = polars_dtype

            return schema

        except Exception as e:
            # Ultimate fallback: metadata only
            logging.warning(f"Schema detection failed, using metadata only: {e}")
            _, meta = pyreadstat.read_sas7bdat(sas_file_path, metadataonly=True, encoding=self.encoding)

            schema = {}
            for col_name in meta.column_names:
                schema[col_name] = "object"  # Default to string

            return schema

    def _detect_schema_from_metadata(self, sas_file_path: str) -> dict:
        _, meta = pyreadstat.read_sas7bdat(sas_file_path, metadataonly=True, encoding=self.encoding)
        raw_types = dict(meta.readstat_variable_types)
        original_formats = dict(meta.original_variable_types)

        # Override raw types with SAS format info when available
        for col_name, sas_format in original_formats.items():
            if sas_format:
                mapped = self._sas_format_to_type(sas_format)
                if mapped:
                    raw_types[col_name] = mapped

        return raw_types

    @staticmethod
    def _sas_format_to_type(sas_format: str) -> str | None:
        """Map SAS format string to a type string recognized by _convert_type_to_keboola."""
        fmt = sas_format.upper().rstrip("0123456789.")
        if fmt in (
            "DATE",
            "DDMMYY",
            "MMDDYY",
            "YYMMDD",
            "EURDFDE",
            "JULDAY",
            "JULIAN",
            "MONYY",
            "QTR",
            "WEEKDATE",
            "WORDDATE",
            "YYMM",
            "YYMON",
            "YYQ",
        ):
            return "date"
        if fmt in ("DATETIME", "DATEAMPM", "DTDATE", "DTMONYY", "DTWKDATX", "DTYEAR", "DTYYQC"):
            return "timestamp"
        if fmt in ("TIME", "HHMM", "HOUR", "MMSS", "TIMEAMPM", "TOD"):
            return "timestamp"
        return None

    def _convert_sas_to_csv_optimized(
        self,
        sas_file_path: str,
        csv_file_path: str,
        incremental_field: str | None = None,
        last_incremental_value: float | int | None = None,
    ) -> int:
        """
        Convert SAS file to CSV using Polars output from pyreadstat.

        Optimizations:
        - Direct Polars output from pyreadstat
        - Polars native write_csv for fast I/O
        - Large chunk size (100k rows)
        - Automatic datetime conversion for proper date formatting

        Args:
            sas_file_path: Path to input SAS file
            csv_file_path: Path to output CSV file
            incremental_field: Column name for incremental filtering (optional)
            last_incremental_value: Last incremental value to filter from (optional)

        Returns:
            Total number of rows converted
        """
        logging.info(f"Converting SAS to CSV with chunk size {self.batch_size:,}")

        if incremental_field and last_incremental_value is not None:
            logging.info(f"Incremental mode: filtering {incremental_field} > {last_incremental_value}")

        # Read metadata to identify date/datetime columns that pyreadstat may fail to convert
        _, file_meta = pyreadstat.read_sas7bdat(sas_file_path, metadataonly=True, encoding=self.encoding)
        sas_date_cols = set()
        sas_datetime_cols = set()
        for col_name, sas_format in dict(file_meta.original_variable_types).items():
            if sas_format:
                mapped = self._sas_format_to_type(sas_format)
                if mapped == "date":
                    sas_date_cols.add(col_name)
                elif mapped == "timestamp":
                    sas_datetime_cols.add(col_name)

        # Create iterator for chunked reading with Polars output
        reader = pyreadstat.read_file_in_chunks(
            pyreadstat.read_sas7bdat,
            sas_file_path,
            chunksize=self.batch_size,
            disable_datetime_conversion=False,
            output_format="polars",
            encoding=self.encoding,
        )

        total_rows = 0
        chunk_num = 0
        first_chunk = True

        for df, meta in reader:
            chunk_num += 1
            chunk_start = time.time()

            # Apply incremental filter if specified
            if incremental_field and last_incremental_value is not None:
                if incremental_field in df.columns:
                    # Convert Unix timestamp to datetime for proper comparison with SAS datetime columns
                    # This handles the case where SAS datetimes are converted to datetime objects
                    last_value_dt = datetime.fromtimestamp(last_incremental_value)

                    # Try to filter - handle both datetime and numeric columns
                    try:
                        df = df.filter(pl.col(incremental_field) > last_value_dt)
                    except Exception:
                        # Fallback: if datetime comparison fails, try numeric comparison
                        logging.warning(
                            f"Datetime comparison failed for '{incremental_field}', trying numeric comparison"
                        )
                        df = df.filter(pl.col(incremental_field) > last_incremental_value)
                else:
                    logging.warning(f"Incremental field '{incremental_field}' not found in data, skipping filter")

            # Skip empty chunks after filtering
            if df.height == 0:
                continue

            # Convert numeric columns that should be dates but weren't converted by pyreadstat
            # SAS dates = days since 1960-01-01, SAS datetimes = seconds since 1960-01-01
            sas_epoch = datetime(1960, 1, 1)
            for col in df.columns:
                col_dtype = str(df[col].dtype).lower()
                if col in sas_date_cols and ("float" in col_dtype or "int" in col_dtype):
                    df = df.with_columns(
                        pl.col(col)
                        .cast(pl.Int64, strict=False)
                        .map_elements(
                            lambda x: (sas_epoch + timedelta(days=x)).strftime("%Y-%m-%d") if x is not None else None,
                            return_dtype=pl.Utf8,
                        )
                        .alias(col)
                    )
                elif col in sas_datetime_cols and ("float" in col_dtype or "int" in col_dtype):
                    df = df.with_columns(
                        pl.col(col)
                        .cast(pl.Int64, strict=False)
                        .map_elements(
                            lambda x: (sas_epoch + timedelta(seconds=x)).strftime("%Y-%m-%d %H:%M:%S")
                            if x is not None
                            else None,
                            return_dtype=pl.Utf8,
                        )
                        .alias(col)
                    )

            # Format date and datetime columns to YYYY-MM-DD
            for col in df.columns:
                col_dtype = str(df[col].dtype)
                if "datetime" in col_dtype.lower() or "date" in col_dtype.lower():
                    df = df.with_columns(pl.col(col).dt.strftime("%Y-%m-%d").alias(col))

            # Replace null values with empty string (NULL in CSV)
            if self.null_values:
                for col in df.columns:
                    if df[col].dtype == pl.Utf8 or df[col].dtype == pl.String:
                        df = df.with_columns(
                            pl.when(pl.col(col).is_in(self.null_values)).then(None).otherwise(pl.col(col)).alias(col)
                        )

            # Write using Polars native CSV writer
            if first_chunk:
                # First chunk: write with header
                df.write_csv(csv_file_path, include_header=True)
                first_chunk = False
            else:
                # Append subsequent chunks without header
                with open(csv_file_path, "ab") as f:
                    df.write_csv(f, include_header=False)

            rows_in_chunk = df.height
            total_rows += rows_in_chunk

            chunk_duration = time.time() - chunk_start
            logging.info(f"Chunk {chunk_num}: {rows_in_chunk:,} rows (total: {total_rows:,}) in {chunk_duration:.2f}s")

        return total_rows

    def convert_schema_to_keboola(self, schema_dict: dict, primary_key_columns: list[str] | None = None) -> OrderedDict:
        """
        Convert schema dictionary to Keboola schema format.

        Args:
            schema_dict: Schema dictionary (column_name -> type_string)
            primary_key_columns: List of column names to mark as primary keys (optional)

        Returns:
            OrderedDict mapping column names to ColumnDefinition objects
        """
        primary_key_columns = primary_key_columns or []

        schema = OrderedDict()
        for col_name, type_string in schema_dict.items():
            schema[col_name] = ColumnDefinition(
                data_types=BaseType(dtype=self._convert_type_to_keboola(str(type_string))),
                primary_key=col_name in primary_key_columns,
                nullable=col_name not in primary_key_columns,
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
