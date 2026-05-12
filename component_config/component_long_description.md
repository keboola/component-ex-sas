# SAS File Extractor

Loads SAS (`.sas7bdat`) tables from an SFTP server into Keboola Storage.

## Key features

- **Row-based**: one config row per SAS table. Use the **Re-load tables** action to populate the picker from the SFTP folder.
- **Chunked streaming**: SAS files are read with pyreadstat in configurable batches and written to CSV by Polars — memory usage stays bounded for very large tables.
- **Data-level incremental load**: pick a column (int, float, date, datetime, or string) and only rows whose value is greater than the last seen value will be loaded. The maximum is persisted in the row state file.
- **Encoding control**: choose the iconv-compatible encoding (`CP1250`, `CP1252`, `UTF-8`, `LATIN1`, `LATIN2`) that matches the SAS export.
- **NULL handling**: map arbitrary string literals (`"NA"`, `"."`, …) to NULL in string columns.
- **SAS date / datetime support**: format hints from the SAS file are honored even when pyreadstat returns the column as numeric.
- **Sync actions**: `testConnection`, `list_sas_tables`, and `prepareRows` (bulk row generation).

## How it works

For each configuration row, the component:

1. Connects to the SFTP server with paramiko.
2. Downloads `<table>.sas7bdat` to a temporary local file.
3. Detects the schema (1000-row pyreadstat sample by default, or SAS metadata only).
4. Streams the file in chunks via pyreadstat → Polars → CSV.
5. Applies the optional incremental filter, tracks the column max, and writes the new state.
6. Writes the manifest with the detected schema, primary key, and load type.

## Use cases

- Pulling periodic SAS exports into Keboola for analytics or warehousing.
- Migrating SAS-based pipelines onto Keboola.
- Incremental ingestion from SAS tables that grow over time.

## Requirements

- SFTP access with username + password authentication.
- Absolute path to a folder containing `.sas7bdat` files.
