# SAS File Extractor

Keboola Connection extractor that loads SAS (`.sas7bdat`) files from an SFTP server into Storage as tables.

**Table of Contents:**

[TOC]

## Overview

The component connects to an SFTP server, downloads a `.sas7bdat` file, and converts it to a Keboola Storage table. It is a row-based component — each configuration row corresponds to one SAS table. SFTP credentials and a few file-level options live on the root config and are shared across all rows.

Internally the component uses **pyreadstat** to parse SAS files into **Polars** DataFrames, then streams chunks through Polars' native CSV writer.

### Key features

- Per-row table selection via a `list_sas_tables` sync action against the SFTP folder.
- `testConnection` sync action validates SFTP credentials from the UI.
- `prepareRows` sync action generates one row per table for bulk setup.
- Chunked, memory-bounded conversion (configurable batch size).
- Optional **data-level incremental load** via a user-chosen column; the maximum value is persisted in the row's state file and reused on the next run.
- Configurable SAS file encoding (handles CP1250 / WLATIN2 and similar legacy encodings).
- Custom strings can be mapped to NULL.
- SAS date/datetime columns are detected via pyreadstat plus SAS format overrides; numeric date/timestamp columns that pyreadstat does not auto-convert are converted using the SAS epoch (1960-01-01).

## Architecture

```
┌───────────────────────────────────────────────────────────┐
│ For each config row:                                      │
│                                                           │
│  1. Connect to SFTP (paramiko)                            │
│  2. Download <table>.sas7bdat to a local temp file        │
│  3. Detect schema:                                        │
│       • sample first 1000 rows with pyreadstat (default), │
│         apply SAS format overrides for date / timestamp   │
│       • or use SAS metadata only (infer_dtypes=false)     │
│  4. Stream chunks (pyreadstat → polars.DataFrame):        │
│       • apply incremental filter if configured            │
│       • track max value of the incremental column         │
│       • convert SAS-epoch numeric dates to date / string  │
│       • format Date / Datetime columns                    │
│       • map null_values strings to NULL                   │
│  5. Append each chunk to <table>.csv via Polars           │
│  6. Write the manifest (schema, PK, load type)            │
│  7. Update state with the new max incremental value       │
└───────────────────────────────────────────────────────────┘
```

## Configuration

### Root configuration

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `sftp.host` | yes | — | SFTP server hostname or IP |
| `sftp.port` | no | `22` | SFTP port |
| `sftp.username` | yes | — | SFTP username |
| `sftp.#password` | yes | — | SFTP password (encrypted) |
| `sftp.folder_path` | yes | — | Absolute path on the SFTP server that contains `.sas7bdat` files |
| `encoding` | no | `CP1250` | iconv-compatible encoding name. Allowed: `CP1250`, `CP1252`, `UTF-8`, `LATIN1`, `LATIN2`. |
| `null_values` | no | `[]` | List of string literals to convert to NULL in string columns (e.g. `["NA", "N/A", "."]`). |

### Row configuration

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `table` | yes | — | SAS filename (e.g. `customers.sas7bdat`). Populated via the **Re-load tables** sync action. |
| `destination.load_type` | no | `full_load` | `full_load` overwrites the Storage table on every run; `incremental_load` appends/upserts. |
| `destination.primary_key` | no | `null` | Columns marked as PK in the manifest. |
| `incremental_column` | no | `null` | Column used for data-level incremental filtering. Only rows whose value is strictly greater than the value stored in state are loaded. Supports int, float, decimal, date, datetime, and string columns; also works on SAS-epoch numeric date/datetime columns. |
| `batch_size` | no | `10000` | pyreadstat chunk size. Lower → less memory, slower. |
| `infer_dtypes` | no | `true` | When `true`, sample 1000 rows to infer types and merge with SAS format hints. When `false`, rely entirely on SAS metadata + format hints. |
| `datetime_as_date` | no | `false` | When `true`, datetime columns are written as `YYYY-MM-DD` (the time part is dropped). Default keeps the full `YYYY-MM-DD HH:MM:SS`. |
| `debug` | no | `false` | Currently a no-op placeholder. |

### Example configuration

Root:

```json
{
  "sftp": {
    "host": "sftp.example.com",
    "port": 22,
    "username": "sas_user",
    "#password": "KBC::ProjectSecure::sftpPassword",
    "folder_path": "/data/sas_exports"
  },
  "encoding": "CP1250",
  "null_values": ["NA", "N/A", "."]
}
```

Row:

```json
{
  "table": "customers.sas7bdat",
  "destination": {
    "load_type": "incremental_load",
    "primary_key": ["id"]
  },
  "incremental_column": "last_updated",
  "batch_size": 10000,
  "infer_dtypes": true,
  "datetime_as_date": false
}
```

## Incremental loading

Set `incremental_column` to enable data-level incremental loading. On each run:

1. The component reads `last_incremental_value` from the row's state file.
2. Each chunk is filtered to rows where `incremental_column > last_incremental_value`.
3. The maximum value of `incremental_column` across all chunks is written back to state.

State stores the value as a string in column-native form (`"2024-01-15"` for dates, `"2024-01-15 13:42:00"` for datetimes, the literal number for ints/floats). SAS-epoch numeric date/datetime columns are translated to ISO date strings in state, then back to SAS-epoch numbers during filtering — comparisons stay in the column's native type.

Combine `incremental_column` with `destination.load_type = "incremental_load"` so new rows are appended to Storage, not overwritten.

## Output

Each row produces one Storage table:

- **Table name**: SAS filename without the `.sas7bdat` extension.
- **Manifest schema**: derived from pyreadstat dtypes plus SAS format hints (see below).
- **Primary key**: `destination.primary_key` if provided.
- **Load mode**: `destination.load_type`.

### Type mapping

The component derives a type string per column (from a Polars sample or from SAS metadata), then maps it to a Keboola type via substring match:

| Detected type string contains | Keboola type |
|-------------------------------|--------------|
| `int` | `INTEGER` |
| `decimal`, `numeric` | `NUMERIC` |
| `float`, `double`, `real` | `FLOAT` |
| `bool` | `BOOLEAN` |
| `datetime`, `timestamp` | `TIMESTAMP` |
| `date` | `DATE` |
| anything else | `STRING` |

SAS format overrides apply for both `infer_dtypes` modes. Recognized SAS date formats (DATE, DDMMYY, MMDDYY, YYMMDD, JULIAN, MONYY, WEEKDATE, …) map to `DATE`; recognized datetime formats (DATETIME, DATEAMPM, DTDATE, …) map to `TIMESTAMP`.

## Sync actions

| Action | Purpose |
|--------|---------|
| `testConnection` | Opens an SFTP connection with the configured credentials. Used by the **Test connection** button on the root config. |
| `list_sas_tables` | Lists `.sas7bdat` files in `sftp.folder_path` and populates the table selector on rows. |
| `prepareRows` | Returns one row template per entry in `init_tables` (used for bulk row generation from the UI). |

## Development

### Local development

```bash
git clone https://github.com/keboola/component-ex-sas component-sas
cd component-sas
docker-compose build
docker-compose run --rm dev
```

The component reads its config from `data/config.json` and writes output to `data/out/tables/`.

### Running tests

```bash
docker-compose run --rm test
```

The image runs `ruff check .` and `python -m unittest discover` (see `scripts/build_n_test.sh`).

### Project layout

```
component-sas/
├── src/
│   ├── component.py                 # ComponentBase entrypoint, sync actions
│   ├── configuration.py             # Pydantic configuration models
│   ├── sftp_client.py               # paramiko-based SFTP wrapper
│   └── sas_to_csv_converter.py      # pyreadstat → Polars → CSV pipeline
├── tests/
│   └── test_component.py            # Unit tests
├── component_config/                # Schemas + UI descriptions
├── pyproject.toml                   # uv-managed dependencies
└── Dockerfile
```

## Troubleshooting

### `SFTP authentication failed`
Verify username/password. Confirm the SFTP server accepts password authentication and that the source IP is allowed.

### `SFTP folder not found`
`sftp.folder_path` must be an absolute path that the user can list. Trailing slashes are normalized; the literal root `/` is preserved.

### `Cannot apply incremental filter on '<col>': stored value '<value>' is incompatible with column type '<dtype>'`
The state file holds a value that can't be parsed against the column's current dtype (e.g. the column type changed in the source). Delete the state file or correct the type, then re-run.

### Mojibake / unreadable text in string columns
The default `encoding` is `CP1250` (used by Czech / Slovak / Hungarian SAS exports). For UTF-8 SAS files set `encoding` to `UTF-8` on the root config.

### Manifest type doesn't match the CSV contents
This generally points to a SAS column whose original format isn't in the built-in format map. Open an issue with the SAS format name (`PROC CONTENTS` output is enough).

## Integration

For deployment instructions, see the [Keboola developer documentation](https://developers.keboola.com/extend/component/deployment/).

## Support

- Bug reports / feature requests: [GitHub issues](https://github.com/keboola/component-ex-sas)
- Product feedback: [ideas.keboola.com](https://ideas.keboola.com/)
- Customer support: support@keboola.com
