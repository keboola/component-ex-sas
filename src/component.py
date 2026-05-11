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
            batch_size=self.params.batch_size,
            null_values=self.params.null_values,
            encoding=self.params.encoding,
            infer_dtypes=self.params.infer_dtypes,
            datetime_as_date=self.params.datetime_as_date,
        )

        # Load last incremental value from state file (stored as string in column-native format)
        self._last_incremental_value: str | None = None
        if self.params.incremental_column:
            state = self.get_state_file()
            if state:
                stored = state.get("last_incremental_value")
                if stored is not None:
                    self._last_incremental_value = str(stored)
                    logging.info(f"Loaded last incremental value: {self._last_incremental_value}")

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
            row_count, new_incremental_value = self._process_sas_file(
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

            # Save state if incremental column is specified — preserve previous value if no new max found
            if self.params.incremental_column:
                final_value = (
                    new_incremental_value if new_incremental_value is not None else self._last_incremental_value
                )
                if final_value is not None:
                    self.write_state_file({"last_incremental_value": final_value})
                    logging.info(f"Saved state: last_incremental_value = {final_value}")

        finally:
            self.sftp_client.close()

    def _process_sas_file(
        self,
        sftp_client: SftpClient,
        converter: SasToCsvConverter,
        sas_file: str,
    ) -> tuple[int, str | None]:
        """
        Process a single SAS file: infer schema first, then create table definition and write CSV.
        """
        table_name = self.params.get_table_name(sas_file)
        sftp_url = sftp_client.get_sftp_url(sas_file)

        logging.info(f"Processing {sas_file}")

        # Step 1: Infer schema from SAS file (downloads and analyzes structure)
        temp_file, schema_dict = converter.infer_sas_schema(
            sftp_url=sftp_url,
            table_name=table_name,
            sftp_client=sftp_client,
        )

        # Step 2: Convert schema to Keboola format
        keboola_schema = converter.convert_schema_to_keboola(
            schema_dict, primary_key_columns=self.params.destination.primary_key
        )
        logging.debug(f"Converted schema: {keboola_schema}")

        # Step 3: Create output table definition WITH schema
        out_table = self.create_out_table_definition(
            f"{table_name}.csv",
            incremental=self.params.destination.incremental,
            has_header=True,
            schema=keboola_schema,
        )

        # Step 4: Convert SAS to CSV (uses already downloaded temp file)
        row_count, new_incremental_value = converter.convert_sas_to_csv(
            temp_file=temp_file,
            output_path=out_table.full_path,
            incremental_field=self.params.incremental_column,
            last_incremental_value=self._last_incremental_value if self.params.incremental_column else None,
        )
        logging.info(f"Successfully processed {sas_file}")

        if row_count == 0:
            return 0, new_incremental_value

        # Step 5: Write manifest
        self.write_manifest(out_table)

        logging.info(f"Successfully streamed {row_count:,} rows to '{table_name}.csv'")
        return row_count, new_incremental_value

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

    @sync_action("prepareRows")
    def prepare_rows(self):
        rows = []
        for table in self.params.init_tables:
            row = {
                "name": table,
                "description": f"SAS table: {table}",
                "configuration": {"parameters": {"table": table}},
            }
            rows.append(row)

        return rows


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
