from __future__ import annotations

import io
import json
import os
import traceback
import uuid
from typing import Any, Callable

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from fastapi import BackgroundTasks, FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from langchain_groq import ChatGroq

from security import (
    add_stripe_fee_fuzzy_fields,
    hitl_header_mapping_system_prompt,
    mask_pii_in_dataframe_with_audit,
    normalize_hitl_llm_mapping,
    sample_masked_dataframe_for_llm,
)

# Schema sampling: LLM sees only a small row slice; full file is mapped with pandas.
LARGE_FILE_BYTES = 5 * 1024 * 1024  # 5MB — use minimal rows for LLM prompt
LLM_SAMPLE_ROWS_NORMAL = 10
LLM_SAMPLE_ROWS_LARGE_FILE = 3
RESPONSE_PREVIEW_ROW_LIMIT = 100

# ``task_id`` -> ``{status, progress, result, error}``. Single-process only:
# not shared across workers; clients poll ``GET /api/status/{task_id}``. No
# persistence—acceptable for local demos, wrong for multi-instance production.
TASK_STORE: dict[str, dict[str, Any]] = {}


# Load environment variables from .env as early as possible
load_dotenv()
groq_api_key = os.getenv("GROQ_API_KEY")


app = FastAPI(title="Micro Reconciliation API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


def _extract_json_object_from_llm(content: str) -> dict[str, Any]:
    """Strip optional markdown fences and parse the LLM JSON object."""
    text = (content or "").strip()
    if text.startswith("```"):
        lines = text.split("\n")
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip().startswith("```"):
            lines = lines[:-1]
        text = "\n".join(lines).strip()
    parsed = json.loads(text)
    if not isinstance(parsed, dict):
        raise ValueError("LLM response JSON was not an object.")
    return parsed


def _map_headers_with_llama_3_1(markdown_preview: str) -> dict[str, Any]:
    """
    Map messy CSV headers to the canonical ledger schema using Llama 3.1.

    Model output is untrusted. We accept only a JSON object. The prompt
    (from ``security.hitl_header_mapping_system_prompt``) asks for per-field
    ``source_column``, ``confidence_score`` (0–100), and ``status``; the
    endpoint normalizes scores and re-derives ``status`` from strict thresholds.

    Fallback logic is enforced in the endpoint: mappings are applied only
    when the referenced source column exists in the uploaded DataFrame.
    If the model hallucinates column names, those mappings are ignored and
    the standardized fields remain ``None``/``N/A``.

    Semantic matching is used instead of rule-based mapping because fintech
    CSV headers vary by provider (abbreviations, punctuation, ordering). Rules
    require constant updates and fail on new header variants.

    Parameters
    ----------
    markdown_preview:
        A small redacted markdown table (headers + sample rows) derived from
        the uploaded CSV. Typically 10 rows (or 3 when the upload exceeds
        5MB). Redaction must happen before calling the model.

    Returns
    -------
    dict[str, Any]
        Raw HITL mapping object from the model (parsed JSON).
    """
    system_instruction = hitl_header_mapping_system_prompt()

    user_message = (
        "Here is the CSV sample in markdown format:\n\n"
        f"{markdown_preview}\n\n"
        "Return only the mapped JSON object as specified."
    )

    llm = ChatGroq(model="llama-3.1-8b-instant")
    response = llm.invoke(
        [
            ("system", system_instruction),
            ("user", user_message),
        ]
    )
    content = getattr(response, "content", None)
    if not content:
        raise ValueError("Empty response from LLM.")

    return _extract_json_object_from_llm(content)


def _set_task_progress(task_id: str, progress: int) -> None:
    """Clamp and write pipeline progress for polling clients.

    Idempotent if ``task_id`` is missing (e.g. race during teardown). Values
    are integers in ``[0, 100]`` so the UI can render a deterministic bar.

    Args:
        task_id: UUID string returned in the 202 response.
        progress: Monotonic-ish percentage from the worker (clamped here).
    """
    if task_id in TASK_STORE:
        TASK_STORE[task_id]["progress"] = min(100, max(0, int(progress)))


def run_reconcile_pipeline(
    raw: bytes,
    on_progress: Callable[[int], None] | None = None,
) -> dict[str, Any]:
    """Execute the full reconcile path on uploaded bytes (sync; CPU/IO bound).

    Stages: UTF-8 decode → ``read_csv`` → PII mask (regex, local) → bounded
    schema sample → Groq/Llama HITL JSON → :func:`normalize_hitl_llm_mapping`
    → pandas column rename and value normalization →
    :func:`add_stripe_fee_fuzzy_fields` → preview slice (``RESPONSE_PREVIEW_ROW_LIMIT``).

    ``on_progress``, when provided, receives coarse percentages (0, 5, 10, 50,
    90, 100) for the async worker to persist in ``TASK_STORE``; it must stay
    cheap (no I/O).

    Raises:
        ValueError: Input/parse/mask errors that should surface as a failed task
            with a client-readable message (bad encoding, empty frame, etc.).
        RuntimeError: Downstream failures (LLM, normalization) where the client
            still sees ``failed`` but the message may be technical.

    Returns:
        Dict with ``mapped_data``, ``total_rows``, ``hitl_mapping``, ``audit_trail``
        (same contract as the pre-async API body under ``result``).
    """
    def p(percent: int) -> None:
        if on_progress:
            on_progress(percent)

    p(0)

    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ValueError("CSV must be UTF-8 encoded.") from exc

    try:
        df = pd.read_csv(io.StringIO(text))
    except Exception as exc:
        raise ValueError(f"Failed to parse CSV: {exc}") from exc

    p(5)

    try:
        masked_df, audit_trail = mask_pii_in_dataframe_with_audit(df)
    except ValueError as exc:
        raise ValueError(str(exc)) from exc
    except Exception as exc:
        raise RuntimeError(f"Failed to mask PII: {exc}") from exc

    large_file = len(raw) > LARGE_FILE_BYTES
    llm_row_count = LLM_SAMPLE_ROWS_LARGE_FILE if large_file else LLM_SAMPLE_ROWS_NORMAL
    try:
        preview_df = sample_masked_dataframe_for_llm(masked_df, llm_row_count)
        markdown_preview = preview_df.to_markdown(index=False)
    except Exception as exc:
        raise RuntimeError(f"Failed to build markdown preview: {exc}") from exc

    audit_trail.append(
        {
            "action": "schema_sampling",
            "llm_sample_rows": llm_row_count,
            "total_rows_in_file": int(len(masked_df)),
            "large_file_threshold_bytes": LARGE_FILE_BYTES,
            "large_file_prompt_trim": large_file,
        }
    )

    try:
        raw_mapping = _map_headers_with_llama_3_1(markdown_preview)
    except Exception as exc:
        raise RuntimeError(f"LLM mapping failed: {exc}") from exc

    try:
        field_sources, hitl_mapping = normalize_hitl_llm_mapping(
            raw_mapping, masked_df.columns
        )
    except Exception as exc:
        raise RuntimeError(f"HITL mapping normalization failed: {exc}") from exc

    p(10)

    lowest_confidence = min(
        int(hitl_mapping[f]["confidence_score"]) for f in hitl_mapping
    )
    audit_trail.append(
        {
            "action": "hitl_evaluation",
            "lowest_confidence_score": lowest_confidence,
        }
    )

    for target_field, meta in hitl_mapping.items():
        audit_trail.append(
            {
                "action": "header_mapping",
                "original_col": meta.get("source_column"),
                "mapped_to": target_field,
                "confidence_score": meta.get("confidence_score"),
                "status": meta.get("status"),
                "reason": "Semantic match",
            }
        )

    rename_map: dict[str, str] = {}
    for target_field, source_name in field_sources.items():
        if isinstance(source_name, str) and source_name in masked_df.columns:
            rename_map[source_name] = target_field

    standardized_df = masked_df.rename(columns=rename_map)

    if "transaction_type" not in standardized_df.columns:
        standardized_df["transaction_type"] = "N/A"
    if "date" not in standardized_df.columns:
        standardized_df["date"] = None
    if "amount" not in standardized_df.columns:
        standardized_df["amount"] = None
    if "description" not in standardized_df.columns:
        standardized_df["description"] = None

    p(50)

    try:
        dates = pd.to_datetime(
            standardized_df["date"],
            errors="coerce",
            format="mixed",
        )
        standardized_df["date"] = dates.dt.strftime("%Y-%m-%d").where(
            dates.notna(), "Invalid Date"
        )
    except Exception as exc:
        raise RuntimeError(f"Failed to normalize date: {exc}") from exc

    try:
        amount_raw = standardized_df["amount"].astype("string")
        amount_clean = amount_raw.str.replace(r"[^\d\.\-]", "", regex=True)
        amount_num = pd.to_numeric(amount_clean, errors="coerce")
        standardized_df["amount"] = amount_num
    except Exception as exc:
        raise RuntimeError(f"Failed to normalize amount: {exc}") from exc

    try:
        standardized_df["transaction_type"] = np.where(
            standardized_df["amount"].isna(),
            "N/A",
            np.where(standardized_df["amount"] >= 0, "credit", "debit"),
        )
    except Exception as exc:
        raise RuntimeError(f"Failed to infer transaction_type: {exc}") from exc

    try:
        standardized_df = add_stripe_fee_fuzzy_fields(standardized_df)
    except Exception as exc:
        raise RuntimeError(f"Failed to compute fee match fields: {exc}") from exc

    p(90)

    standardized_df = standardized_df[
        [
            "date",
            "amount",
            "description",
            "transaction_type",
            "has_fee_discrepancy",
            "suggested_gross",
            "detected_fee",
        ]
    ]

    standardized_df = standardized_df.replace({np.nan: None})
    records = standardized_df.to_dict(orient="records")
    total_rows = len(records)
    if total_rows > RESPONSE_PREVIEW_ROW_LIMIT:
        preview_records = records[:RESPONSE_PREVIEW_ROW_LIMIT]
    else:
        preview_records = records

    p(100)

    return {
        "mapped_data": preview_records,
        "total_rows": total_rows,
        "hitl_mapping": hitl_mapping,
        "audit_trail": audit_trail,
    }


def run_reconcile_background(task_id: str, raw: bytes) -> None:
    """Run :func:`run_reconcile_pipeline` after the HTTP response has been sent.

    FastAPI schedules this in a thread pool via ``BackgroundTasks`` so the
    ``POST /reconcile`` handler returns **202** immediately after reading the
    body—no blocking on LLM or large pandas work. Outcomes are written only to
    ``TASK_STORE[task_id]``: ``completed`` + ``result`` on success, ``failed``
    + ``error`` on ``ValueError`` or any other exception (server errors include
    traceback text for debugging; trim before external exposure if needed).

    Args:
        task_id: Key initialized in ``TASK_STORE`` before enqueue.
        raw: Raw CSV bytes (already fully read in the request handler).

    Note:
        Not durable: process restart drops all tasks. No authentication on
        ``task_id``—treat IDs as unguessable UUIDs only for demo scope.
    """
    try:

        def on_progress(percent: int) -> None:
            _set_task_progress(task_id, percent)

        result = run_reconcile_pipeline(raw, on_progress=on_progress)
        TASK_STORE[task_id] = {
            "status": "completed",
            "progress": 100,
            "result": result,
            "error": None,
        }
    except ValueError as exc:
        TASK_STORE[task_id] = {
            "status": "failed",
            "progress": TASK_STORE.get(task_id, {}).get("progress", 0),
            "result": None,
            "error": str(exc),
        }
    except Exception as exc:
        TASK_STORE[task_id] = {
            "status": "failed",
            "progress": TASK_STORE.get(task_id, {}).get("progress", 0),
            "result": None,
            "error": f"{exc}\n{traceback.format_exc()}",
        }


@app.post("/reconcile")
async def reconcile(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
) -> JSONResponse:
    """Enqueue reconciliation; respond immediately with a pollable task id.

    Validates extension and non-empty body synchronously. Persists bytes only in
    the closure passed to ``BackgroundTasks``—the heavy pipeline runs after the
    response is flushed, so the client does not block on LLM or dataframe work.

    Returns:
        JSONResponse with status **202** and body ``{task_id, status: "processing"}``.

    Note:
        ``TASK_STORE`` must be populated before ``add_task`` so the first poll
        always sees ``processing``. For production, replace with a queue + worker
        and authN on task access.
    """
    filename = file.filename or "upload.csv"
    if not filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Please upload a .csv file.")

    raw = await file.read()
    if not raw:
        raise HTTPException(status_code=400, detail="Uploaded file is empty.")

    task_id = str(uuid.uuid4())
    TASK_STORE[task_id] = {
        "status": "processing",
        "progress": 0,
        "result": None,
        "error": None,
    }
    background_tasks.add_task(run_reconcile_background, task_id, raw)

    return JSONResponse(
        status_code=202,
        content={"task_id": task_id, "status": "processing"},
    )


@app.get("/api/status/{task_id}")
def get_task_status(task_id: str) -> dict[str, Any]:
    """Return the latest snapshot for ``task_id`` (polling endpoint).

    Reads ``TASK_STORE`` only; no side effects. Typical fields: ``status``
    (``processing`` | ``completed`` | ``failed``), ``progress`` (0–100),
    ``result`` (payload when completed), ``error`` (when failed). **404** if the
    id was never issued or expired after restart.

    Args:
        task_id: UUID from the 202 body.

    Returns:
        Mutable dict stored in ``TASK_STORE`` (clients should treat as read-only).

    Raises:
        HTTPException: 404 when unknown.
    """
    if task_id not in TASK_STORE:
        raise HTTPException(status_code=404, detail="Unknown task_id.")
    return TASK_STORE[task_id]


