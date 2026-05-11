# Configuration Guide

The component is row-based. SFTP credentials and a couple of file-level options sit on the **root** configuration; each **row** describes one SAS table to extract.

## 1. Root configuration

### SFTP connection

- **Host** — SFTP hostname or IP.
- **Port** — defaults to 22.
- **Username** / **Password** — SFTP credentials (the password is stored encrypted).
- **Folder Path** — absolute path on the server that contains the `.sas7bdat` files. The literal root path `/` is preserved; trailing slashes are stripped.
- **Test Connection** button — runs the `testConnection` sync action against the supplied credentials.

### File handling

- **SAS File Encoding** — iconv-compatible encoding of the SAS file (`CP1250`, `CP1252`, `UTF-8`, `LATIN1`, `LATIN2`). Default `CP1250` matches WLATIN2 SAS exports common in CEE locales. Pick `UTF-8` for modern SAS files.
- **Null Values** — list of string literals that should be mapped to NULL in string columns (e.g. `"NA"`, `"N/A"`, `"."`).

## 2. Row configuration

### Table

- **Table to extract** — filename of the SAS table (e.g. `customers.sas7bdat`). Use **Re-load tables** to populate the dropdown from the SFTP folder, or type a filename manually if the file isn't listed yet.

### Destination

- **Load Type** — `full_load` overwrites the Storage table; `incremental_load` appends/upserts.
- **Primary Key** — columns marked as the table's primary key in the manifest.

### Incremental column (data-level)

Set **Incremental Column** to a column in the SAS file (timestamp, date, or numeric). On each run, only rows whose value is strictly greater than the previously stored value are loaded, and the new maximum is written back to state.

- Works on int, float, decimal, date, datetime, and string columns.
- Also supports SAS-epoch numeric date/datetime columns (the state value is stored as an ISO date string and translated back to SAS-epoch numbers during filtering).
- For an upsert pattern, combine with `Load Type = incremental_load` and a Primary Key.

### Conversion settings

- **Batch Size** — pyreadstat chunk size (default 10000). Lower values reduce memory usage at the cost of throughput.
- **Infer Data Types** — when enabled (default), the component samples the first 1000 rows with pyreadstat to detect Polars dtypes, then applies SAS format hints to fix date/timestamp columns the sample didn't auto-convert. When disabled, the component relies entirely on SAS metadata and format hints.
- **Output datetimes as dates** — when enabled, datetime columns are written as `YYYY-MM-DD` (time component dropped). Off by default, so full `YYYY-MM-DD HH:MM:SS` timestamps are preserved.
- **Debug Mode** — reserved for future use.

## Type mapping

The detected type string is mapped to a Keboola type by substring matching:

- `int` → `INTEGER`
- `decimal`, `numeric` → `NUMERIC`
- `float`, `double`, `real` → `FLOAT`
- `bool` → `BOOLEAN`
- `datetime`, `timestamp` → `TIMESTAMP`
- `date` → `DATE`
- anything else → `STRING`

Recognized SAS date formats (DATE, DDMMYY, MMDDYY, YYMMDD, JULIAN, MONYY, WEEKDATE, …) are forced to `DATE`; recognized datetime formats (DATETIME, DATEAMPM, DTDATE, …) are forced to `TIMESTAMP`. This keeps the manifest in sync with the values actually written to the CSV.
