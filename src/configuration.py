from enum import Enum

from keboola.component.exceptions import UserException
from pydantic import BaseModel, Field, ValidationError, computed_field, field_validator


class LoadType(str, Enum):
    full_load = "full_load"
    incremental_load = "incremental_load"


class SftpConnection(BaseModel):
    """SFTP connection configuration."""

    host: str
    port: int = 22
    username: str
    password: str = Field(alias="#password")
    folder_path: str = Field(description="Base folder path on SFTP server containing SAS files")

    @field_validator("folder_path")
    def validate_folder_path(cls, v):
        # Strip trailing slashes for consistency, but preserve the root path '/'.
        stripped = v.rstrip("/")
        return stripped or "/"


class Destination(BaseModel):
    load_type: LoadType = Field(default=LoadType.full_load, description="Type of load: full or incremental")
    primary_key: list[str] | None = Field(default=None, description="List of column names to use as primary key")

    @computed_field
    @property
    def incremental(self) -> bool:
        return self.load_type == LoadType.incremental_load


class Configuration(BaseModel):
    """Main component configuration."""

    sftp: SftpConnection
    table: str | None = Field(default=None, description="SAS table file to extract")
    destination: Destination = Field(default_factory=Destination)
    incremental_column: str | None = Field(default=None)
    batch_size: int = Field(default=10000)
    null_values: list[str] = Field(default_factory=list, description="List of strings to treat as NULL values")
    encoding: str = Field(default="CP1250")
    infer_dtypes: bool = Field(default=True)
    datetime_as_date: bool = Field(default=False)
    debug: bool = False
    init_tables: list[str] | None = None

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
