import logging

from keboola.component.exceptions import UserException
from pydantic import BaseModel, Field, ValidationError, field_validator


class SftpConnection(BaseModel):
    """SFTP connection configuration."""

    host: str
    port: int = 22
    username: str
    password: str = Field(alias="#password")
    folder_path: str = Field(description="Base folder path on SFTP server containing SAS files")

    @field_validator("folder_path")
    def validate_folder_path(cls, v):
        # Ensure path doesn't end with trailing slash for consistency
        return v.rstrip("/")


class OutputSettings(BaseModel):
    """Output table configuration."""

    primary_key: list[str] | None = Field(default=None, description="List of column names to use as primary key")
    incremental: bool = Field(default=False, description="Write to Keboola Storage in incremental mode")


class Configuration(BaseModel):
    """Main component configuration."""

    sftp: SftpConnection
    sas_tables: list[str]
    output: OutputSettings = Field(default_factory=OutputSettings)
    duckdb_max_memory_mb: int = 768
    debug: bool = False

    @field_validator("sas_tables")
    def validate_sas_tables(cls, v):
        if not v:
            raise ValueError("At least one SAS file must be specified")

        # Validate all files have .sas7bdat extension
        invalid_files = [f for f in v if not f.endswith(".sas7bdat")]
        if invalid_files:
            raise ValueError(f"All files must have .sas7bdat extension. Invalid files: {invalid_files}")

        return v

    def get_table_name(self, sas_filename: str) -> str:
        """
        Convert SAS filename to Keboola table name.
        Removes .sas7bdat extension.

        Args:
            sas_filename: Name of SAS file (e.g., 'customers.sas7bdat')

        Returns:
            Table name (e.g., 'customers')
        """
        return sas_filename.replace(".sas7bdat", "")
