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

mcp = FastMCP("Clinical Data MIMIC Tools")

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


@mcp.tool(description="Get source records behind a patient or cohort result. evidence_type: patient, admissions, diagnoses or notes. Use the subject_id from the result; optionally select a hadm_id or one note_id (notes only). Returns original rows, source table names and full note text, with pagination.")
def get_evidence(
    subject_id: StrictInt,
    evidence_type: str,
    hadm_id: StrictInt | None = None,
    note_id: str | None = None,
    limit: StrictInt = 20,
    offset: StrictInt = 0,
) -> dict[str, Any]:
    check_integer(subject_id, "subject_id")
    check_integer(limit, "limit", maximum=100)
    check_integer(offset, "offset", minimum=0, maximum=2**31 - 1)
    evidence_type = evidence_type.strip().lower()
    if evidence_type not in {"patient", "admissions", "diagnoses", "notes"}:
        raise ValueError("evidence_type must be patient, admissions, diagnoses or notes.")
    values = {"subject_id": subject_id, "fetch_limit": limit + 1, "offset": offset}
    admission_filter = ""
    if hadm_id is not None:
        check_integer(hadm_id, "hadm_id")
        values["hadm_id"] = hadm_id
        admission_filter = " AND a.hadm_id = :hadm_id"
    if note_id is not None:
        note_id = note_id.strip()
        if evidence_type != "notes" or not note_id:
            raise ValueError("note_id must contain text and can only be used with notes.")
        values["note_id"] = note_id

    if evidence_type == "patient":
        source_tables = ["patients"]
        where = "p.subject_id = :subject_id"
        if hadm_id is not None:
            where += " AND EXISTS (SELECT 1 FROM admissions a WHERE a.subject_id = p.subject_id AND a.hadm_id = :hadm_id)"
        sql = f"SELECT p.* FROM patients p WHERE {where} ORDER BY p.subject_id"
    elif evidence_type == "admissions":
        source_tables = ["admissions"]
        sql = f"""
            SELECT a.* FROM admissions a
            WHERE a.subject_id = :subject_id {admission_filter}
            ORDER BY a.admittime DESC NULLS LAST, a.hadm_id
        """
    elif evidence_type == "diagnoses":
        source_tables = ["diagnoses_icd", "d_icd_diagnoses"]
        sql = f"""
            SELECT d.*, x.long_title AS diagnosis_name
            FROM diagnoses_icd d JOIN admissions a
              ON d.subject_id = a.subject_id AND d.hadm_id = a.hadm_id
            LEFT JOIN d_icd_diagnoses x ON d.icd_code = x.icd_code AND d.icd_version = x.icd_version
            WHERE d.subject_id = :subject_id {admission_filter}
            ORDER BY d.hadm_id, d.seq_num, d.icd_version, d.icd_code
        """
    else:
        source_tables = ["radiology", "discharge"]
        where = "TRUE"
        if hadm_id is not None:
            where += " AND n.hadm_id = :hadm_id AND a.subject_id = n.subject_id"
        if note_id is not None:
            where += " AND n.note_id = :note_id"
        sql = f"""
            WITH notes AS ({NOTES_SQL})
            SELECT n.*, {LINK_STATUS} AS admission_link_status
            FROM notes n LEFT JOIN admissions a ON n.hadm_id = a.hadm_id
            WHERE {where}
            ORDER BY n.charttime DESC NULLS LAST, n.note_id
        """
    rows = query(sql + " LIMIT :fetch_limit OFFSET :offset", values)
    has_more = len(rows) > limit
    return {"subject_id": subject_id, "evidence_type": evidence_type, "source_tables": source_tables,
            "requested_hadm_id": hadm_id, "requested_note_id": note_id, "records": rows[:limit],
            "returned_count": min(len(rows), limit), "has_more": has_more,
            "next_offset": offset + limit if has_more else None}


def cohort_query(diagnosis_keyword, icd_code, icd_version, gender, min_age, max_age, start_date, end_date):
    values, filters, diagnosis_filters = {}, [], []
    if diagnosis_keyword is not None:
        diagnosis_keyword = diagnosis_keyword.strip()
        if not diagnosis_keyword:
            raise ValueError("diagnosis_keyword must contain text, or use null to omit it.")
        values["diagnosis_keyword"] = diagnosis_keyword
        diagnosis_filters.append("instr(lower(x.long_title), lower(:diagnosis_keyword)) > 0")
    if icd_code is not None:
        icd_code = icd_code.strip().upper().replace(".", "")
        if not re.fullmatch(r"[A-Z0-9]{1,7}", icd_code) or icd_version is None:
            raise ValueError("icd_code must be an ICD code with icd_version set to 9 or 10.")
        values["icd_code"] = icd_code
        diagnosis_filters.append("d.icd_code = :icd_code")
    if icd_version is not None:
        check_integer(icd_version, "icd_version", minimum=9, maximum=10)
        values["icd_version"] = icd_version
        diagnosis_filters.append("d.icd_version = :icd_version")
    if diagnosis_filters:
        filters.append(f"""EXISTS (
            SELECT 1 FROM diagnoses_icd d
            LEFT JOIN d_icd_diagnoses x ON d.icd_code = x.icd_code AND d.icd_version = x.icd_version
            WHERE d.subject_id = a.subject_id AND d.hadm_id = a.hadm_id
              AND {' AND '.join(diagnosis_filters)}
        )""")
    if gender is not None:
        gender = gender.strip().upper()
        if gender not in {"F", "M"}:
            raise ValueError("gender must be F or M.")
        values["gender"] = gender
        filters.append("p.gender = :gender")
    age = "p.anchor_age + year(a.admittime) - p.anchor_year"
    for name, value, operator in [("min_age", min_age, ">="), ("max_age", max_age, "<=")]:
        if value is not None:
            check_integer(value, name, minimum=0, maximum=120)
            values[name] = value
            filters.append(f"({age}) {operator} :{name}")
    if min_age is not None and max_age is not None and min_age > max_age:
        raise ValueError("min_age must be on or below max_age.")
    for name, value, operator in [("start_date", start_date, ">="), ("end_date", end_date, "<=")]:
        if value is not None:
            if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
                raise ValueError(f"{name} must use YYYY-MM-DD.")
            date.fromisoformat(value)
            values[name] = value
            filters.append(f"CAST(a.admittime AS DATE) {operator} CAST(:{name} AS DATE)")
    if start_date and end_date and start_date > end_date:
        raise ValueError("start_date must be on or before end_date.")
    where = " AND ".join(filters) or "TRUE"
    sql = f"""WITH cohort AS (
        SELECT a.subject_id, a.hadm_id, p.gender, p.anchor_age, p.anchor_year,
               {age} AS age_at_admission, p.anchor_age = 91 AS age_top_coded,
               a.admittime, a.dischtime, a.admission_type
        FROM admissions a JOIN patients p ON a.subject_id = p.subject_id
        WHERE {where}
    )"""
    return sql, values


@mcp.tool(description="Find a research cohort, one row per matching admission, with patient and admission IDs. Filter by diagnosis dictionary wording (literal substring), exact ICD code plus version, F/M gender, approximate age at admission, or inclusive admission dates (YYYY-MM-DD). All filters apply to the same admission. Ages use anchor_age + admission year - anchor_year; age_top_coded marks patients whose anchor age was replaced with 91. Results are paginated; use the same filters with get_cohort_overview for full-cohort statistics.")
def search_cohort(
    diagnosis_keyword: str | None = None,
    icd_code: str | None = None,
    icd_version: StrictInt | None = None,
    gender: str | None = None,
    min_age: StrictInt | None = None,
    max_age: StrictInt | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    limit: StrictInt = 20,
    offset: StrictInt = 0,
) -> dict[str, Any]:
    check_integer(limit, "limit", maximum=100)
    check_integer(offset, "offset", minimum=0, maximum=2**31 - 1)
    sql, values = cohort_query(diagnosis_keyword, icd_code, icd_version, gender,
                               min_age, max_age, start_date, end_date)
    rows = query(sql + """
        SELECT * FROM cohort ORDER BY subject_id, hadm_id LIMIT :fetch_limit OFFSET :offset
    """, values | {"fetch_limit": limit + 1, "offset": offset})
    has_more = len(rows) > limit
    return {"filters": values, "admissions": rows[:limit], "returned_count": min(len(rows), limit),
            "has_more": has_more, "next_offset": offset + limit if has_more else None}


@mcp.tool(description="Summarize the entire research cohort using the same filters as search_cohort. Returns distinct patient and admission counts, gender counts by patient, approximate age statistics by admission, admission dates, the ten most frequent coded diagnoses, and note counts/dates for matching admissions only. Diagnosis keywords search dictionary wording; codes match exactly with their version. Ages use anchor_age + admission year - anchor_year, including the deidentified 91 age group. Dates filter admission dates, not note dates. Omit filters to summarize all admissions with a patient record.")
def get_cohort_overview(
    diagnosis_keyword: str | None = None,
    icd_code: str | None = None,
    icd_version: StrictInt | None = None,
    gender: str | None = None,
    min_age: StrictInt | None = None,
    max_age: StrictInt | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict[str, Any]:
    sql, values = cohort_query(diagnosis_keyword, icd_code, icd_version, gender,
                               min_age, max_age, start_date, end_date)
    summary = query(sql + """
        SELECT count(DISTINCT subject_id) AS patient_count, count(*) AS admission_count,
               min(admittime) AS first_admission_date, max(admittime) AS last_admission_date,
               count(age_at_admission) AS admissions_with_age,
               min(age_at_admission) AS min_age_at_admission,
               max(age_at_admission) AS max_age_at_admission,
               avg(age_at_admission) AS mean_age_at_admission,
               count(DISTINCT CASE WHEN age_top_coded THEN subject_id END) AS age_top_coded_patient_count
        FROM cohort
    """, values)[0]
    if summary["mean_age_at_admission"] is not None:
        summary["mean_age_at_admission"] = float(summary["mean_age_at_admission"])
    genders = query(sql + """
        SELECT gender, count(DISTINCT subject_id) AS patient_count
        FROM cohort GROUP BY gender ORDER BY gender NULLS LAST
    """, values)
    diagnoses = query(sql + """
        SELECT d.icd_code, d.icd_version, x.long_title AS diagnosis_name,
               count(DISTINCT d.subject_id) AS patient_count,
               count(DISTINCT d.hadm_id) AS admission_count
        FROM cohort c JOIN diagnoses_icd d ON c.subject_id = d.subject_id AND c.hadm_id = d.hadm_id
        LEFT JOIN d_icd_diagnoses x ON d.icd_code = x.icd_code AND d.icd_version = x.icd_version
        GROUP BY d.icd_code, d.icd_version, x.long_title
        ORDER BY patient_count DESC, admission_count DESC, d.icd_version, d.icd_code
        LIMIT 10
    """, values)
    note_groups = query(sql + """, notes AS (
        SELECT 'radiology' AS note_category, n.subject_id, n.hadm_id, n.charttime
        FROM radiology n JOIN cohort c ON n.subject_id = c.subject_id AND n.hadm_id = c.hadm_id
        UNION ALL
        SELECT 'discharge' AS note_category, n.subject_id, n.hadm_id, n.charttime
        FROM discharge n JOIN cohort c ON n.subject_id = c.subject_id AND n.hadm_id = c.hadm_id
    )
        SELECT note_category, count(*) AS note_count,
               count(DISTINCT subject_id) AS patient_count, count(DISTINCT hadm_id) AS admission_count,
               min(charttime) AS first_note_date, max(charttime) AS last_note_date
        FROM notes GROUP BY note_category ORDER BY note_category
    """, values)
    notes_by_type = {kind: {"note_count": 0, "patient_count": 0, "admission_count": 0,
                            "first_note_date": None, "last_note_date": None}
                     for kind in ("radiology", "discharge")}
    for group in note_groups:
        notes_by_type[group.pop("note_category")] = group
    return {"filters": values, **summary, "patients_by_gender": genders,
            "top_diagnoses": diagnoses, "notes_by_type": notes_by_type,
            "note_scope": "Notes linked to matching admissions by both subject_id and hadm_id."}


if __name__ == "__main__":
    mcp.run(transport="stdio")
