# Clinical Data MCP

This version includes two physician tools, two researcher tools and one shared tool for checking source records.

## What it does

- **`search_patient_notes`** finds a patient's radiology reports and discharge summaries. It supports filters for note type, date, keyword and admission ID, and returns the original text with record IDs and dates. Results are shown newest first, with 20 notes per page by default.
- **`get_patient_overview`** returns patient information, admissions, recorded diagnoses, and note counts and date ranges. You can select one admission using `hadm_id`. Notes without a verified admission are listed separately.
- **`search_cohort`** finds admissions matching diagnosis, gender, age and admission date filters. Each row contains a patient ID and admission ID, demographic fields and admission dates. Results are ordered by patient ID and admission ID, with 20 admissions per page by default.
- **`get_cohort_overview`** summarizes all admissions matching the same filters. It returns distinct patient and admission counts, gender counts, age statistics, admission dates, the ten most frequent coded diagnoses, and note counts and dates.
- **`get_evidence`** retrieves the source records behind a result. Choose `patient`, `admissions`, `diagnoses` or `notes` with `evidence_type`. It returns original fields and source table names, with diagnosis descriptions for coded diagnoses and full original text for notes. Both physicians and researchers can use it to inspect an individual patient from a previous result.

All tools query the MIMIC tables in Databricks. The physician and evidence tools require `subject_id`; the researcher tools use cohort filters.

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

Example arguments for `search_cohort`:

```json
{"icd_code": "1970", "icd_version": 9, "min_age": 50, "limit": 10}
```

Use the same filters with `get_cohort_overview`, without pagination arguments:

```json
{"icd_code": "1970", "icd_version": 9, "min_age": 50}
```

Both researcher tools accept `diagnosis_keyword`, `icd_code`, `icd_version`, `gender` (`F` or `M`), `min_age`, `max_age`, `start_date` and `end_date`. Omit filters to include all admissions with a patient record. All supplied conditions must match the same admission. A patient can have several matching admissions; the overview counts distinct patients separately.

`diagnosis_keyword` searches diagnosis dictionary titles using a case-insensitive literal substring. An ICD code is an exact match and requires version `9` or `10`; dots are removed, so `197.0` and `1970` match the same code. If both a keyword and code are supplied, they must match the same diagnosis. These filters select recorded diagnoses, including history codes when their wording matches.

Dates use `YYYY-MM-DD` and filter the admission date, including both endpoints. They are the shifted dates stored in MIMIC. Age limits are inclusive integers from 0 to 120, applied to `age_at_admission = anchor_age + year(admittime) - anchor_year`. This is an approximate age. MIMIC replaces anchor ages over 89 with 91; these patients are marked `age_top_coded`. Age filters and statistics include those substituted values. See the [MIMIC age fields](https://mimic.mit.edu/docs/iv/modules/hosp/patients.html).

`search_cohort` supports `limit` (1–100) and `offset`. Follow `next_offset` for more admissions. The overview covers the full matching cohort regardless of search page size. Gender counts use distinct patients; age statistics use admissions with an age value; top diagnoses include all coded diagnoses on matching admissions. Note statistics cover only notes linked to those admissions by both IDs, including notes dated outside the admission-date filter. For other notes belonging to a patient, use `search_patient_notes` with that patient's ID.

Use the returned `subject_id` and `hadm_id` with `get_patient_overview` or `get_evidence` to inspect a cohort member.

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
