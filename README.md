# Clinical Data MCP

This version includes two tools for physician queries and one shared tool for checking source records.

## What it does

- **`search_patient_notes`** finds a patient's radiology reports and discharge summaries. It supports filters for note type, date, keyword and admission ID, and returns the original text with record IDs and dates. Results are shown newest first, with 20 notes per page by default.
- **`get_patient_overview`** returns patient information, admissions, recorded diagnoses, and note counts and date ranges. You can select one admission using `hadm_id`. Notes without a verified admission are listed separately.
- **`get_evidence`** retrieves the source records behind a result. Choose `patient`, `admissions`, `diagnoses` or `notes` with `evidence_type`. It returns original fields and source table names, with diagnosis descriptions for coded diagnoses and full original text for notes. Both physicians and researchers can use it to inspect an individual patient from a previous result.

All three tools require a patient ID (`subject_id`) and query the MIMIC tables in Databricks.

## How to use

Run the commands below from this folder.

Install dependencies with Python 3.11 or newer:

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
```

Log into Databricks using the CLI with access to the MIMIC tables. For a new setup, copy the example config and fill in your profile, warehouse, catalog and schema:

```sh
cp .env.example .env
```

Keep `.env` on your computer. It is excluded from Git; `.env.example` is the shared template.

Start the server:

```sh
./run_mcp.sh
```

The server waits for MCP requests. To connect a client, use `mcp_client_config.json` with this folder as the client's server working directory. Its paths are relative to this folder.

Example arguments for `search_patient_notes`:

```json
{"subject_id": 10000032, "note_type": "radiology", "limit": 5}
```

Optional filters are `note_type` (`radiology` or `discharge`), `start_date`, `end_date`, `keyword` and `hadm_id`. Dates use `YYYY-MM-DD`, including both endpoints. Use the returned `next_offset` as `offset` to get the next page.

Example arguments for `get_patient_overview`:

```json
{"subject_id": 10000032, "hadm_id": 22595853}
```

Example arguments for `get_evidence`:

```json
{"subject_id": 10000032, "evidence_type": "diagnoses", "hadm_id": 22595853}
```

To retrieve one original note:

```json
{"subject_id": 10000032, "evidence_type": "notes", "note_id": "10000032-RR-54"}
```

`hadm_id` limits evidence to a verified admission for that patient. `note_id` is only used with `notes` and must belong to the selected patient and admission, if provided. Without `hadm_id`, notes with missing or unmatched admission IDs are retained and labeled with their link status. Results use `records`, `returned_count`, `has_more` and `next_offset`. The default page size is 20; `limit` can be 1–100. Follow `next_offset` to retrieve the remaining records.

Evidence is read from the current database using the supplied IDs. To check a filtered search, use the note IDs from that search. The tool does not store earlier calls or decide whether a generated statement is correct.
