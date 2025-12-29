"""
SAS File Extractor Component.

Extracts SAS (.sas7bdat) files from SFTP server and writes to Keboola Storage as CSV.
"""

import logging
from datetime import datetime

from keboola.component.base import ComponentBase, sync_action
from keboola.component.exceptions import UserException
from keboola.component.sync_actions import SelectElement

from configuration import Configuration
from sas_to_csv_converter import SasToCsvConverter
from sftp_client import SftpClient


class Component(ComponentBase):
    """
    SAS File Extractor component.
    """

    def __init__(self):
        super().__init__()
        self.params = Configuration(**self.configuration.parameters)

        self.sftp_client = SftpClient(self.params.sftp)
        self.converter = SasToCsvConverter(
            max_memory_mb=self.params.duckdb_max_memory_mb, batch_size=self.params.batch_size
        )

    def run(self):
        start_time = datetime.now()

        try:
            self.sftp_client.connect()

            # Check if table is specified
            if not self.params.table:
                raise UserException("No table specified. Please select a table to extract.")

            sas_file = self.params.table
            logging.info(f"Processing file: {sas_file}")

            # Process the file
            row_count = self._process_sas_file(
                sftp_client=self.sftp_client,
                converter=self.converter,
                sas_file=sas_file,
            )

            # Summary
            duration = (datetime.now() - start_time).total_seconds()
            if row_count > 0:
                logging.info(f"Extraction complete: {row_count:,} rows extracted in {duration:.2f} seconds")
            else:
                logging.info(f"No data found in {sas_file}")

        finally:
            # Clean up connections
            self.converter.close()
            self.sftp_client.close()

    def _process_sas_file(
        self,
        sftp_client: SftpClient,
        converter: SasToCsvConverter,
        sas_file: str,
    ) -> int:
        """
        Process a single SAS file: stream directly from SAS to CSV for Keboola.
        """
        table_name = self.params.get_table_name(sas_file)
        sftp_url = sftp_client.get_sftp_url(sas_file)

        # Create output table definition first (to get path)
        out_table = self.create_out_table_definition(
            f"{table_name}.csv",
            incremental=self.params.output.incremental,
            has_header=True,
        )
        logging.info(f"Processing {sas_file}")

        # Stream SAS file directly to CSV and get schema
        row_count, polars_schema = converter.load_sas_file_and_convert_to_csv(
            sftp_url=sftp_url,
            table_name=table_name,
            output_path=out_table.full_path,
            sftp_client=sftp_client,
        )
        logging.info(f"Successfully processed {sas_file}")

        if row_count == 0:
            return 0

        # Convert Polars schema to Keboola schema
        schema = converter.convert_schema_to_keboola(polars_schema)

        # Update table definition with schema
        out_table.schema = schema

        # Write manifest
        self.write_manifest(out_table)

        logging.info(f"Successfully streamed {row_count:,} rows to '{table_name}.csv'")
        return row_count

    @sync_action("list_sas_tables")
    def list_sas_tables(self):
        """Sync action to list SAS files from SFTP server."""
        try:
            self.sftp_client.connect()

            try:
                sas_tables = self.sftp_client.list_sas_files()
                return [SelectElement(label=f, value=f) for f in sas_tables]
            finally:
                self.sftp_client.close()

        except Exception as e:
            raise UserException(f"Failed to list SAS files: {e}")

    @sync_action("testConnection")
    def test_connection(self):
        """Sync action to test SFTP connection."""
        try:
            self.sftp_client.connect()
        except Exception as e:
            raise UserException(f"Connection test failed: {str(e)}")
        finally:
            self.sftp_client.close()


if __name__ == "__main__":
    try:
        comp = Component()
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
