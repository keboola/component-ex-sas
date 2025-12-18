# SAS File Extractor

Keboola Connection extractor component for loading SAS (.sas7bdat) files from SFTP servers.

**Table of Contents:**

[TOC]

## Overview

This component extracts SAS files from SFTP servers using DuckDB's `read_stat` extension and loads them into Keboola Storage as tables. It streams data directly from SFTP without creating local temporary files, making it memory-efficient for large datasets.

### Key Features

- **Direct SFTP Streaming**: Reads SAS files directly from SFTP server without local downloads
- **DuckDB Integration**: Uses DuckDB's `read_stat` extension for efficient SAS file parsing
- **Dual Incremental Loading**:
  - **File-Level**: Only processes new or modified files based on modification timestamps
  - **Data-Level**: Within files, only loads rows with timestamps newer than last processing
- **Automatic Schema Detection**: Automatically detects column types and maps to Keboola types
- **Dynamic File Selection**: Sync action to list available SAS files from SFTP server
- **Memory Efficient**: Configurable memory limits with automatic spill-to-disk

## How It Works

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│ 1. Connect to SFTP Server                                   │
│    • paramiko (for listing files)                           │
│    • fsspec (for DuckDB streaming)                          │
└──────────────┬──────────────────────────────────────────────┘
               │
               ↓
┌─────────────────────────────────────────────────────────────┐
│ 2. Initialize DuckDB                                        │
│    • Install read_stat extension from community             │
│    • Register SFTP filesystem                               │
└──────────────┬──────────────────────────────────────────────┘
               │
               ↓
┌─────────────────────────────────────────────────────────────┐
│ 3. For Each SAS File:                                       │
│                                                              │
│    ┌───────────────────────────────────────────────┐       │
│    │ Check File-Level Incremental                  │       │
│    │  • Compare modification timestamps             │       │
│    │  • Skip if unchanged                           │       │
│    └───────────┬───────────────────────────────────┘       │
│                ↓                                             │
│    ┌───────────────────────────────────────────────┐       │
│    │ Stream SAS File → DuckDB                      │       │
│    │  • read_stat() reads via fsspec SFTP          │       │
│    │  • Apply data-level filter if configured      │       │
│    │  • No temporary files created                 │       │
│    └───────────┬───────────────────────────────────┘       │
│                ↓                                             │
│    ┌───────────────────────────────────────────────┐       │
│    │ Export DuckDB Table → CSV                     │       │
│    │  • Detect schema and map types                │       │
│    │  • Write with Keboola manifest                │       │
│    └───────────┬───────────────────────────────────┘       │
│                ↓                                             │
│    ┌───────────────────────────────────────────────┐       │
│    │ Update State                                   │       │
│    │  • Save file modification timestamp            │       │
│    │  • Save max data timestamp                     │       │
│    └───────────────────────────────────────────────┘       │
└─────────────────────────────────────────────────────────────┘
```

### Data Flow

1. **SFTP Connection**: Component establishes secure SFTP connection using credentials
2. **File Discovery**: Lists all `.sas7bdat` files in specified folder
3. **Incremental Check**: Compares file modification times with stored state
4. **Streaming Load**: DuckDB reads SAS files directly over SFTP network connection
5. **Schema Detection**: DuckDB analyzes SAS metadata and converts to SQL types
6. **Export**: Data written to CSV with proper Keboola manifest for import
7. **State Update**: Component saves processing state for next incremental run

## Configuration

### SFTP Connection

| Parameter | Required | Description |
|-----------|----------|-------------|
| Host | Yes | SFTP server hostname or IP address |
| Port | No | SFTP port (default: 22) |
| Username | Yes | SFTP username |
| Password | Yes | SFTP password (encrypted) |
| Folder Path | Yes | Path to folder containing SAS files (e.g., `/data/sas_files`) |

### SAS Files

Use the **"List Files"** button to populate available SAS files from the SFTP server. The component will display all `.sas7bdat` files in the configured folder.

### Incremental Loading

#### File-Level Incremental (default: enabled)

- Tracks modification timestamps of processed files
- Only processes files that are new or have been modified since last run
- State stored in Keboola's state file

#### Data-Level Incremental (default: disabled)

- Within each file, only loads rows with timestamps newer than last processing
- Requires specifying a **Timestamp Column** in the SAS file
- Combines with file-level incremental for maximum efficiency

### Output Settings

| Parameter | Default | Description |
|-----------|---------|-------------|
| Primary Key | None | Column(s) to use as primary key in Keboola Storage |
| Incremental Write | false | Write to Storage in incremental mode (append/update) |
| Preserve Insertion Order | true | Maintain row order from source SAS files |

### Advanced Settings

| Parameter | Default | Description |
|-----------|---------|-------------|
| DuckDB Maximum Memory (MB) | 1024 | Memory limit for DuckDB (128-16384 MB) |
| Debug Mode | false | Enable detailed debug logging |

## Example Configuration

```json
{
  "sftp": {
    "host": "sftp.example.com",
    "port": 22,
    "username": "sas_user",
    "#password": "encrypted_password",
    "folder_path": "/data/sas_exports"
  },
  "sas_files": [
    "customers.sas7bdat",
    "orders.sas7bdat",
    "products.sas7bdat"
  ],
  "incremental": {
    "file_level_incremental": true,
    "data_level_incremental": true,
    "timestamp_column": "last_updated"
  },
  "output": {
    "primary_key": ["customer_id"],
    "incremental": true
  },
  "duckdb_max_memory_mb": 2048,
  "debug": false
}
```

## Output

The component creates one Keboola Storage table per SAS file:

- **Table Name**: Derived from SAS filename (e.g., `customers.sas7bdat` → `customers` table)
- **Schema**: Automatically detected from SAS file metadata
- **Type Mapping**: SAS types converted to Keboola supported types

### Type Conversion

| SAS/DuckDB Type | Keboola Type |
|-----------------|--------------|
| INTEGER, BIGINT, SMALLINT | INTEGER |
| DECIMAL, NUMERIC | NUMERIC |
| DOUBLE, FLOAT | FLOAT |
| BOOLEAN | BOOLEAN |
| TIMESTAMP | TIMESTAMP |
| DATE | DATE |
| VARCHAR, TEXT, Others | STRING |

## Incremental Loading Behavior

### Example Scenarios

#### Scenario 1: File-Level Only

```yaml
Configuration:
  file_level_incremental: true
  data_level_incremental: false

First Run:
  - customers.sas7bdat (modified: 2024-01-01 10:00)  → Processed (1000 rows)
  - orders.sas7bdat (modified: 2024-01-01 11:00)     → Processed (5000 rows)

Second Run (no changes):
  - customers.sas7bdat (modified: 2024-01-01 10:00)  → Skipped
  - orders.sas7bdat (modified: 2024-01-01 11:00)     → Skipped

Third Run (customers updated):
  - customers.sas7bdat (modified: 2024-01-02 14:00)  → Processed (1050 rows)
  - orders.sas7bdat (modified: 2024-01-01 11:00)     → Skipped
```

#### Scenario 2: File-Level + Data-Level

```yaml
Configuration:
  file_level_incremental: true
  data_level_incremental: true
  timestamp_column: "last_updated"

First Run:
  - customers.sas7bdat → Processed (1000 rows, max timestamp: 2024-01-01 12:00)

Second Run (file modified, new data added):
  - customers.sas7bdat → Processed (50 NEW rows where last_updated > 2024-01-01 12:00)
```

## Development

### Local Development Setup

1. Clone the repository:
```bash
git clone https://github.com/keboola/component-ex-sas component-sas
cd component-sas
```

2. Build the Docker image:
```bash
docker-compose build
```

3. Create local configuration in `data/config.json`

4. Run the component:
```bash
docker-compose run --rm dev
```

### Running Tests

Execute the test suite:
```bash
docker-compose run --rm test
```

This runs:
- Unit tests for configuration validation
- State management tests
- Type conversion tests
- Integration tests (mocked)

### Project Structure

```
component-sas/
├── src/
│   ├── component.py           # Main component logic
│   ├── configuration.py       # Pydantic configuration models
│   ├── sftp_manager.py        # SFTP connection handling
│   ├── duckdb_manager.py      # DuckDB operations
│   └── state_manager.py       # Incremental state tracking
├── tests/
│   └── test_component.py      # Unit tests
├── component_config/
│   ├── configSchema.json      # UI configuration schema
│   └── ...                    # Component metadata
├── pyproject.toml             # Python dependencies
└── Dockerfile                 # Container definition
```

## Troubleshooting

### Connection Issues

**Problem**: `SFTP authentication failed`

**Solution**:
- Verify username and password are correct
- Ensure SFTP server allows password authentication
- Check if IP whitelisting is required

---

**Problem**: `SFTP folder not found`

**Solution**:
- Verify folder path is absolute (starts with `/`)
- Check folder exists and is accessible with provided credentials
- Ensure proper permissions on SFTP folder

### Performance Issues

**Problem**: `Out of memory errors`

**Solution**:
- Increase `duckdb_max_memory_mb` setting
- Process fewer files per run
- Enable file-level incremental to reduce data volume

---

**Problem**: `Slow processing for large files`

**Solution**:
- Network speed between component and SFTP server affects performance
- DuckDB automatically spills to disk for large datasets
- Consider splitting very large SAS files if possible

### Data Issues

**Problem**: `Column type mismatch`

**Solution**:
- Component automatically detects types from SAS metadata
- Check SAS file schema if unexpected type conversions occur
- Types default to STRING if cannot be mapped

## Integration

For deployment and integration with Keboola Connection, refer to the [deployment section of the developer documentation](https://developers.keboola.com/extend/component/deployment/).

## Support

For issues, feature requests, or questions:
- Submit issues to [GitHub repository](https://github.com/keboola/component-ex-sas)
- Feature requests: [ideas.keboola.com](https://ideas.keboola.com/)
- Keboola Support: support@keboola.com
