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
    table: str | None = Field(default=None, description="SAS table file to extract")
    output: OutputSettings = Field(default_factory=OutputSettings)
    duckdb_max_memory_mb: int = 768
    batch_size: int = Field(
        default=10000, description="Number of rows to process at once (lower = less memory, slower)"
    )
    debug: bool = False

    def __init__(self, **data):
        try:
            super().__init__(**data)
        except ValidationError as e:
            error_messages = [f"{err['loc']}: {err['msg']}" for err in e.errors()]
            raise UserException(f"Configuration validation error: {', '.join(error_messages)}")

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
