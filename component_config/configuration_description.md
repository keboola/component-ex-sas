# Configuration Guide

## Quick Start

1. **Configure SFTP Connection**
   - Enter your SFTP server hostname
   - Provide username and password (encrypted)
   - Specify the folder path containing SAS files

2. **Select SAS Files**
   - Click "List Files" button to see available files
   - Select which files to extract

3. **Configure Incremental Loading** (Optional)
   - Enable file-level incremental to track processed files
   - Enable data-level incremental for timestamp-based filtering

4. **Set Output Options**
   - Define primary keys for your tables
   - Choose incremental write mode if needed

## Incremental Loading

### File-Level Incremental (Recommended)

Enabled by default. Tracks modification timestamps of processed files and skips unchanged files on subsequent runs.

**Benefits:**
- Reduces processing time
- Lowers SFTP data transfer
- Minimizes costs

### Data-Level Incremental (Advanced)

For SAS files that are updated with new rows, configure a timestamp column to load only recent data.

**Requirements:**
- SAS file must have a timestamp/date column
- Column must be consistently ordered
- Specify column name in configuration

**Example:** If your SAS file has a `last_updated` timestamp column, the component will only load rows where `last_updated` is newer than the previous run.

## Output Configuration

### Primary Key

Specify columns that uniquely identify rows. Used by Keboola Storage for:
- Deduplication
- Incremental updates (upserts)
- Data quality checks

### Incremental Write

When enabled with primary key:
- New rows are inserted
- Existing rows (matching primary key) are updated
- Efficient for regularly updated data

When disabled (full load):
- Table is completely replaced each run
- Simpler, suitable for smaller datasets

## Advanced Settings

### DuckDB Memory Limit

Controls maximum RAM usage. DuckDB automatically spills to disk if data exceeds memory.

**Recommendations:**
- Small files (<100MB): 512 MB
- Medium files (100MB-1GB): 1024 MB (default)
- Large files (>1GB): 2048-4096 MB

### Debug Mode

Enables detailed logging for troubleshooting connection issues or data problems.