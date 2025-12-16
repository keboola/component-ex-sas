# SAS File Extractor

Extract SAS (.sas7bdat) files from SFTP servers and load them into Keboola Storage.

## Key Features

- **Direct SFTP Streaming**: Reads SAS files directly without local downloads
- **DuckDB Integration**: Uses DuckDB's read_stat extension for efficient parsing
- **Dual Incremental Loading**:
  - File-level: Track and process only new/modified files
  - Data-level: Within files, load only new rows based on timestamp
- **Automatic Schema Detection**: Column types automatically detected and mapped
- **Dynamic File Discovery**: Sync action to browse and select SAS files
- **Memory Efficient**: Configurable memory limits with disk spill-over

## How It Works

The component:
1. Connects to your SFTP server securely
2. Lists available SAS files in specified folder
3. Streams files directly from SFTP into DuckDB
4. Automatically detects schemas and types
5. Exports data to Keboola Storage tables
6. Tracks state for efficient incremental runs

## Use Cases

- Regular extraction of SAS statistical files
- Migrating SAS data warehouses to Keboola
- Processing large SAS datasets with limited memory
- Incremental updates from SAS-based systems

## Requirements

- SFTP server with SAS (.sas7bdat) files
- SFTP credentials (username/password)
- Path to folder containing SAS files