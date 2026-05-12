"""
Converter for SAS files to CSV format.
Uses pyreadstat with Polars output for memory-efficient processing of large files.
"""

import logging
import os
import time
from collections import OrderedDict
from datetime import date, datetime, timedelta

import polars as pl
import pyreadstat
from keboola.component.dao import BaseType, ColumnDefinition, SupportedDataTypes
from keboola.component.exceptions import UserException

from sftp_client import SftpClient

TEMP_DIR = os.path.join(os.environ.get("TMPDIR", "/tmp"), "sas_converter")


class SasToCsvConverter:
    """
    Converter for SAS files to CSV format.

    Reads SAS files with pyreadstat (Polars output), streams chunks to CSV via Polars'
    native CSV writer.
    """

    def __init__(
        self,
        batch_size: int = 100000,
        null_values: list[str] | None = None,
        encoding: str | None = None,
        infer_dtypes: bool = True,
        datetime_as_date: bool = False,
    ):
        """
        Args:
            batch_size: Number of rows to process at once (default: 100k for optimal performance)
            null_values: List of strings to treat as NULL values
            encoding: Encoding override for SAS files (iconv-compatible name, e.g. 'CP1250' for WLATIN2)
            infer_dtypes: If True, infer types from data sample. If False, use SAS metadata types.
            datetime_as_date: If True, truncate datetime columns to date (YYYY-MM-DD) on output.

        Raises:
            UserException: If initialization fails
        """
        self.batch_size = batch_size
        self.null_values = null_values or []
        self.encoding = encoding
        self.infer_dtypes = infer_dtypes
        self.datetime_as_date = datetime_as_date

        try:
            os.makedirs(TEMP_DIR, exist_ok=True)
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
        2. Detect schema by sampling the first 1000 rows (or from SAS metadata only)

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
                schema_dict = self._detect_schema_from_sample(temp_file)
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
        last_incremental_value: str | None = None,
    ) -> tuple[int, str | None]:
        """
        Convert already downloaded SAS file to CSV using optimized pyreadstat chunking.

        Args:
            temp_file: Path to temporary SAS file (from infer_sas_schema)
            output_path: Path where CSV should be written (from table definition)
            incremental_field: Column name for incremental filtering (optional)
            last_incremental_value: Last incremental value (column-native string form) to filter from

        Returns:
            Tuple of (total_rows, new_max_incremental_value_as_string)

        Raises:
            UserException: If conversion fails
        """
        try:
            # Convert SAS to CSV using optimized pyreadstat
            start_convert = time.time()
            total_rows, new_max_value = self._convert_sas_to_csv_optimized(
                temp_file, output_path, incremental_field, last_incremental_value
            )

            total_duration = time.time() - start_convert
            if total_rows > 0:
                logging.info(
                    f"Converted {total_rows:,} rows to CSV in {total_duration:.2f} seconds "
                    f"({total_rows / total_duration:.0f} rows/sec)"
                )

            return total_rows, new_max_value

        except UserException:
            raise
        except Exception as e:
            raise UserException(f"Failed to convert SAS file to CSV: {e}") from e
        finally:
            # Clean up temp file
            if os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                except Exception as e:
                    logging.warning(f"Failed to remove temp file: {e}")

    def _detect_schema_from_sample(self, sas_file_path: str) -> dict:
        """
        Detect schema from SAS file using pyreadstat with Polars output.

        For numeric columns whose SAS original format indicates a date or timestamp,
        override the Polars dtype with the SAS-format-derived type so the manifest
        matches what _convert_sas_to_csv_optimized actually writes (which manually
        converts those numeric columns to date/datetime strings using
        meta.original_variable_types).

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

            sas_format_overrides = {
                col: self._sas_format_to_type(fmt) for col, fmt in dict(meta.original_variable_types).items() if fmt
            }

            schema = {}
            for col_name in df.columns:
                polars_dtype = str(df[col_name].dtype)
                # If the column is still numeric but the SAS format says it's a
                # date/timestamp, the runtime path will emit date strings — match
                # that in the manifest.
                if ("int" in polars_dtype.lower() or "float" in polars_dtype.lower()) and sas_format_overrides.get(
                    col_name
                ) in ("date", "timestamp"):
                    schema[col_name] = sas_format_overrides[col_name]
                else:
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
    def _parse_incremental_threshold(
        value_str: str,
        col_name: str,
        col_dtype: pl.DataType,
        sas_date_cols: set,
        sas_datetime_cols: set,
    ):
        """
        Parse stored incremental value (string) into a value comparable against the column's native dtype.

        SAS date/datetime columns left as numeric by pyreadstat are compared in SAS units
        (days/seconds since 1960-01-01). All other columns use the column's native Python type.
        """
        sas_epoch = datetime(1960, 1, 1)
        try:
            if col_name in sas_date_cols:
                d = date.fromisoformat(value_str)
                return float((datetime.combine(d, datetime.min.time()) - sas_epoch).days)
            if col_name in sas_datetime_cols:
                dt = datetime.fromisoformat(value_str)
                return float((dt - sas_epoch).total_seconds())

            dtype_str = str(col_dtype).lower()
            if "datetime" in dtype_str:
                return datetime.fromisoformat(value_str)
            if "date" in dtype_str:
                return date.fromisoformat(value_str)
            if "int" in dtype_str:
                # Parse as int to preserve precision for values > 2^53.
                # Tolerate stored floats like "42.0" by routing through float first.
                try:
                    return int(value_str)
                except ValueError:
                    return int(float(value_str))
            if any(t in dtype_str for t in ("float", "decimal")):
                return float(value_str)
            return value_str
        except (ValueError, TypeError) as e:
            raise UserException(
                f"Cannot apply incremental filter on '{col_name}': "
                f"stored value '{value_str}' is incompatible with column type '{col_dtype}' ({e})"
            ) from e

    @staticmethod
    def _format_incremental_value(
        value,
        col_name: str,
        sas_date_cols: set,
        sas_datetime_cols: set,
    ) -> str:
        """Format a native max value back to a string suitable for state storage."""
        sas_epoch = datetime(1960, 1, 1)
        if col_name in sas_date_cols:
            return (sas_epoch + timedelta(days=int(value))).date().isoformat()
        if col_name in sas_datetime_cols:
            return (sas_epoch + timedelta(seconds=float(value))).isoformat()
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, date):
            return value.isoformat()
        return str(value)

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
        last_incremental_value: str | None = None,
    ) -> tuple[int, str | None]:
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
            Tuple of (total_rows, new_max_incremental_value_as_string). The second element is
            `None` when no incremental field is configured or no rows were observed.
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
        running_max = None  # Native max value of incremental column across all chunks

        for df, _ in reader:
            chunk_num += 1
            chunk_start = time.time()

            # Apply incremental filter against the column's native dtype
            if incremental_field:
                if incremental_field not in df.columns:
                    if chunk_num == 1:
                        logging.warning(f"Incremental field '{incremental_field}' not found in data, skipping filter")
                else:
                    if last_incremental_value is not None:
                        threshold = self._parse_incremental_threshold(
                            last_incremental_value,
                            incremental_field,
                            df[incremental_field].dtype,
                            sas_date_cols,
                            sas_datetime_cols,
                        )
                        df = df.filter(pl.col(incremental_field) > threshold)

                    # Track max value of incremental column in native dtype
                    if df.height > 0:
                        chunk_max = df[incremental_field].max()
                        if chunk_max is not None and (running_max is None or chunk_max > running_max):
                            running_max = chunk_max

            # Skip empty chunks after filtering
            if df.height == 0:
                continue

            # Convert numeric columns that should be dates but weren't converted by pyreadstat.
            # SAS dates = days since 1960-01-01, SAS datetimes = seconds since 1960-01-01.
            # Vectorized via pl.duration; nulls propagate naturally.
            sas_epoch_lit = pl.lit(datetime(1960, 1, 1))
            sas_date_exprs = []
            sas_datetime_exprs = []
            for col in df.columns:
                col_dtype = str(df[col].dtype).lower()
                if not ("float" in col_dtype or "int" in col_dtype):
                    continue
                if col in sas_date_cols:
                    sas_date_exprs.append(
                        (sas_epoch_lit + pl.duration(days=pl.col(col).cast(pl.Int64, strict=False)))
                        .cast(pl.Date)
                        .alias(col)
                    )
                elif col in sas_datetime_cols:
                    # Format directly to string so the next strftime loop skips it.
                    datetime_fmt = "%Y-%m-%d" if self.datetime_as_date else "%Y-%m-%d %H:%M:%S"
                    sas_datetime_exprs.append(
                        (sas_epoch_lit + pl.duration(seconds=pl.col(col).cast(pl.Int64, strict=False)))
                        .dt.strftime(datetime_fmt)
                        .alias(col)
                    )
            if sas_date_exprs:
                df = df.with_columns(sas_date_exprs)
            if sas_datetime_exprs:
                df = df.with_columns(sas_datetime_exprs)

            # Apply all per-chunk transformations in a single with_columns call.
            # Polars dtype selectors fan out to all matching columns inside the engine,
            # so there is no Python-level iteration over columns regardless of table width.
            datetime_fmt = "%Y-%m-%d" if self.datetime_as_date else "%Y-%m-%d %H:%M:%S"
            transform_exprs = [
                pl.col(pl.Date).dt.strftime("%Y-%m-%d"),
                pl.col(pl.Datetime).dt.strftime(datetime_fmt),
            ]
            if self.null_values:
                transform_exprs.append(pl.col(pl.String).replace(self.null_values, None))
            df = df.with_columns(transform_exprs)

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

        new_max_str: str | None = None
        if running_max is not None and incremental_field:
            new_max_str = self._format_incremental_value(
                running_max, incremental_field, sas_date_cols, sas_datetime_cols
            )

        return total_rows, new_max_str

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
