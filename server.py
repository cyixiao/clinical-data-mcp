import json
import os
import re
import time
from datetime import date
from functools import lru_cache
from pathlib import Path
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP
from pydantic import StrictInt

load_dotenv(Path(__file__).with_name(".env"))

mcp = FastMCP("Physician MIMIC Tools")

NOTES_SQL = """
    SELECT 'radiology' AS note_category, * FROM radiology WHERE subject_id = :subject_id
    UNION ALL
    SELECT 'discharge' AS note_category, * FROM discharge WHERE subject_id = :subject_id
"""
LINK_STATUS = """CASE
    WHEN n.hadm_id IS NULL THEN 'missing_id'
    WHEN a.hadm_id IS NULL THEN 'unmatched'
    WHEN a.subject_id <> n.subject_id THEN 'patient_mismatch'
    ELSE 'matched' END"""


@lru_cache(maxsize=1)
def workspace():
    return WorkspaceClient(profile=os.environ["DATABRICKS_CONFIG_PROFILE"])


def query(sql, values):
    api = workspace().statement_execution
    parameters = [StatementParameterListItem(
        name=name, value=str(value),
        type="INT" if name in {"fetch_limit", "offset"} else
             "BIGINT" if isinstance(value, int) else "STRING",
    ) for name, value in values.items()]
    response = api.execute_statement(
        warehouse_id=os.environ["DATABRICKS_WAREHOUSE_ID"],
        catalog=os.environ["DATABRICKS_CATALOG"],
        schema=os.environ["DATABRICKS_SCHEMA"],
        statement=sql, parameters=parameters, wait_timeout="30s",
    )
    deadline = time.monotonic() + 300
    while response.status.state.value in {"PENDING", "RUNNING"}:
        if time.monotonic() >= deadline:
            api.cancel_execution(response.statement_id)
            raise TimeoutError("Databricks query timed out. Please try again.")
        time.sleep(1)
        response = api.get_statement(response.statement_id)
    if response.status.state.value != "SUCCEEDED":
        error = response.status.error
        raise RuntimeError(f"Databricks query failed: {error.message if error else response.status.state.value}")
    if response.manifest.truncated:
        raise RuntimeError("Databricks truncated the result. Use a smaller page or one admission.")

    columns = response.manifest.schema.columns
    rows, chunk = [], response.result
    while chunk:
        for values in chunk.data_array or []:
            row = {}
            for column, value in zip(columns, values):
                kind = column.type_name.value if column.type_name else column.type_text.upper()
                if value is not None and kind in {"BYTE", "SHORT", "INT", "LONG", "BIGINT"}:
                    value = int(value)
                elif value is not None and kind in {"ARRAY", "STRUCT", "MAP", "BOOLEAN"}:
                    value = json.loads(value)
                row[column.name] = value
            rows.append(row)
        chunk = (api.get_statement_result_chunk_n(response.statement_id, chunk.next_chunk_index)
                 if chunk.next_chunk_index is not None else None)
    return rows


def check_integer(value, name, minimum=1, maximum=2**63 - 1):
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError(f"{name} must be an integer from {minimum} to {maximum}.")


@mcp.tool(description="Search a patient's radiology or discharge notes by keyword, date or admission.")
def search_patient_notes(
    subject_id: StrictInt,
    note_type: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    keyword: str | None = None,
    hadm_id: StrictInt | None = None,
    limit: StrictInt = 20,
    offset: StrictInt = 0,
) -> dict[str, Any]:
    check_integer(subject_id, "subject_id")
    check_integer(limit, "limit", maximum=100)
    check_integer(offset, "offset", minimum=0, maximum=2**31 - 1)
    values = {"subject_id": subject_id, "fetch_limit": limit + 1, "offset": offset}
    filters = []
    if hadm_id is not None:
        check_integer(hadm_id, "hadm_id")
        values["hadm_id"] = hadm_id
        filters.append("n.hadm_id = :hadm_id AND a.subject_id = n.subject_id")
    if note_type is not None:
        note_type = note_type.strip().lower()
        if note_type not in {"radiology", "discharge"}:
            raise ValueError("note_type must be radiology or discharge.")
        values["note_category"] = note_type
        filters.append("n.note_category = :note_category")
    for name, value in [("start_date", start_date), ("end_date", end_date)]:
        if value is not None:
            if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
                raise ValueError(f"{name} must use YYYY-MM-DD.")
            date.fromisoformat(value)
            values[name] = value
    if start_date and end_date and start_date > end_date:
        raise ValueError("start_date must be on or before end_date.")
    if start_date:
        filters.append("CAST(n.charttime AS DATE) >= CAST(:start_date AS DATE)")
    if end_date:
        filters.append("CAST(n.charttime AS DATE) <= CAST(:end_date AS DATE)")
    if keyword is not None:
        keyword = keyword.strip()
        if not keyword:
            raise ValueError("keyword must contain text, or use null to omit it.")
        values["keyword"] = keyword
        filters.append("instr(lower(n.text), lower(:keyword)) > 0")
    where = " AND ".join(filters) or "TRUE"
    rows = query(f"""
        WITH notes AS ({NOTES_SQL}), page AS (
            SELECT n.*, {LINK_STATUS} AS admission_link_status
            FROM notes n LEFT JOIN admissions a ON n.hadm_id = a.hadm_id
            WHERE {where}
            ORDER BY n.charttime DESC NULLS LAST, n.note_id
            LIMIT :fetch_limit OFFSET :offset
        ), details AS (
            SELECT d.* FROM radiology_detail d JOIN page p
              ON d.note_id = p.note_id AND d.subject_id = p.subject_id
              AND p.note_category = 'radiology'
            UNION ALL
            SELECT d.* FROM discharge_detail d JOIN page p
              ON d.note_id = p.note_id AND d.subject_id = p.subject_id
              AND p.note_category = 'discharge'
        ), grouped_details AS (
            SELECT note_id, subject_id, sort_array(collect_list(named_struct(
                'field_name', field_name, 'field_ordinal', field_ordinal,
                'field_value', field_value))) AS details
            FROM details GROUP BY note_id, subject_id
        )
        SELECT p.*, coalesce(d.details, array()) AS details FROM page p
        LEFT JOIN grouped_details d ON p.note_id = d.note_id AND p.subject_id = d.subject_id
        ORDER BY p.charttime DESC NULLS LAST, p.note_id
    """, values)
    for note in rows:
        for detail in note["details"]:
            if detail["field_ordinal"] is not None:
                detail["field_ordinal"] = int(detail["field_ordinal"])
    has_more = len(rows) > limit
    return {"subject_id": subject_id, "notes": rows[:limit], "returned_count": min(len(rows), limit),
            "has_more": has_more, "next_offset": offset + limit if has_more else None}


@mcp.tool(description="Get patient details, admissions, coded diagnoses and note counts.")
def get_patient_overview(subject_id: StrictInt, hadm_id: StrictInt | None = None) -> dict[str, Any]:
    check_integer(subject_id, "subject_id")
    if hadm_id is not None:
        check_integer(hadm_id, "hadm_id")
    patient = query("SELECT * FROM patients WHERE subject_id = :subject_id", {"subject_id": subject_id})
    values = {"subject_id": subject_id}
    admission_filter = "a.subject_id = :subject_id"
    note_filter = "TRUE"
    if hadm_id is not None:
        values["hadm_id"] = hadm_id
        admission_filter += " AND a.hadm_id = :hadm_id"
        note_filter = "n.hadm_id = :hadm_id AND a.subject_id = n.subject_id"
    admissions = query(f"""
        SELECT a.subject_id, a.hadm_id, a.admittime, a.dischtime,
               a.admission_type, a.discharge_location
        FROM admissions a WHERE {admission_filter}
        ORDER BY a.admittime DESC NULLS LAST, a.hadm_id
    """, values)
    diagnoses = query(f"""
        SELECT d.hadm_id, d.seq_num, d.icd_code, d.icd_version, x.long_title AS diagnosis_name
        FROM diagnoses_icd d JOIN admissions a
          ON d.subject_id = a.subject_id AND d.hadm_id = a.hadm_id
        LEFT JOIN d_icd_diagnoses x ON d.icd_code = x.icd_code AND d.icd_version = x.icd_version
        WHERE {admission_filter}
        ORDER BY d.hadm_id, d.seq_num, d.icd_version, d.icd_code
    """, values)
    note_groups = query(f"""
        WITH notes AS ({NOTES_SQL})
        SELECT n.hadm_id, n.note_category, {LINK_STATUS} AS admission_link_status,
               count(*) AS note_count, min(n.charttime) AS first_note_date,
               max(n.charttime) AS last_note_date
        FROM notes n LEFT JOIN admissions a ON n.hadm_id = a.hadm_id
        WHERE {note_filter}
        GROUP BY n.hadm_id, n.note_category, {LINK_STATUS}
        ORDER BY n.hadm_id NULLS FIRST, n.note_category
    """, values)

    by_admission = {row["hadm_id"]: row for row in admissions}
    for admission in admissions:
        admission["diagnoses"] = []
        admission["notes_by_type"] = {
            kind: {"note_count": 0, "first_note_date": None, "last_note_date": None}
            for kind in ("radiology", "discharge")
        }
    for diagnosis in diagnoses:
        by_admission[diagnosis.pop("hadm_id")]["diagnoses"].append(diagnosis)
    unlinked = []
    for group in note_groups:
        if group["admission_link_status"] == "matched":
            by_admission[group["hadm_id"]]["notes_by_type"][group["note_category"]] = {
                key: group[key] for key in ("note_count", "first_note_date", "last_note_date")
            }
        else:
            unlinked.append(group)
    return {"subject_id": subject_id, "patient_record_available": bool(patient),
            "patient": patient[0] if patient else None, "requested_hadm_id": hadm_id,
            "admissions": admissions, "unlinked_notes": unlinked}


if __name__ == "__main__":
    mcp.run(transport="stdio")
