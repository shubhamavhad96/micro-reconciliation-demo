from __future__ import annotations

import io
import json
import os
from typing import Any

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from langchain_groq import ChatGroq

from security import mask_pii_in_dataframe_with_audit


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


def _map_headers_with_llama_3_1(markdown_preview: str) -> dict[str, Any]:
    """
    Map messy CSV headers to the canonical ledger schema using Llama 3.1.

    Model output is untrusted. We accept only a JSON object and reject empty
    or non-object responses. The prompt asks the model to use ``null`` for
    fields it cannot map with confidence, which becomes our confidence signal.

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
        A small redacted markdown table (headers + a few rows) derived from
        the uploaded CSV. Redaction must happen before calling the model.

    Returns
    -------
    dict[str, Any]
        A mapping object with keys for the canonical fields.
    """
    system_instruction = (
        "You are a financial data normalization assistant. "
        "You are given a markdown table representing CSV headers and sample rows. "
        "Your task is to map the messy CSV headers to the following exact JSON schema:\n\n"
        "{\n"
        '  "date": "YYYY-MM-DD",\n'
        '  "amount": "float",\n'
        '  "description": "string",\n'
        '  "transaction_type": "credit/debit"\n'
        "}\n\n"
        "Return only a single valid JSON object that uses the CSV column names as "
        "values for this schema (e.g. {\"date\": \"txn_date\", ...}). "
        "If a field cannot be confidently mapped, set its value to null. "
        "Do not include any explanations, comments, or extra text—only the JSON object."
    )

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

    mapping = json.loads(content)
    if not isinstance(mapping, dict):
        raise ValueError("LLM response JSON was not an object.")

    return mapping


@app.post("/reconcile")
async def reconcile(file: UploadFile = File(...)) -> dict[str, Any]:
    """
    Reconcile a user-provided CSV into a canonical ledger row shape.

    This endpoint is stateless: it computes transformations and audit
    messages from the uploaded file and returns them in the response payload
    (no database writes).

    Payload constraints
    --------------------
    - Request method: ``POST /reconcile``
    - Content type: ``multipart/form-data``
    - Form field name: ``file``
    - Filename must end with ``.csv``
    - Body is decoded as UTF-8 using ``utf-8-sig`` before parsing with pandas.
    - Maximum upload size is enforced by FastAPI/Starlette and any reverse
      proxy. If the request body is too large, clients may receive ``413``.

    Security controls (SOC2-oriented)
    --------------------------------
    - PII masking happens locally via regex *before* the prompt payload is
      built for the LLM.
    - We prefer regex false positives over data leakage. Over-masking is
      safer than sending raw account numbers or emails to an external model.

    LLM safety and fallback logic
    ------------------------------
    - The LLM output is treated as untrusted.
    - We parse the model output as JSON and reject non-object responses.
    - The endpoint applies mappings only when the referenced source column
      exists in the uploaded DataFrame. If the model hallucinates column
      names, canonical fields stay ``None``/``N/A``.

    HTTP error codes
    -----------------
    - ``400``: malformed input (non-CSV filename, empty upload, UTF-8 decode
      errors, CSV parse errors, or empty/invalid DataFrame for masking and
      normalization).
    - ``413``: request payload too large (FastAPI/Starlette or proxy).
    - ``500``: LLM mapping failures (including provider timeouts), invalid
      JSON from the model, or internal normalization errors.

    Parameters
    ----------
    file:
        Uploaded CSV file provided via multipart/form-data.

    Returns
    -------
    dict[str, Any]
        A response object containing:
        - ``mapped_data``: standardized rows suitable for ledger ingestion.
        - ``audit_trail``: a non-sensitive explanation of masking and header mapping.
    """
    filename = file.filename or "upload.csv"
    if not filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Please upload a .csv file.")

    raw = await file.read()
    if not raw:
        raise HTTPException(status_code=400, detail="Uploaded file is empty.")

    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        raise HTTPException(
            status_code=400,
            detail="CSV must be UTF-8 encoded.",
        )

    # Read CSV into Pandas DataFrame
    try:
        df = pd.read_csv(io.StringIO(text))
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Failed to parse CSV: {exc}")

    # Mask PII using shared helper (+ capture audit events)
    try:
        masked_df, audit_trail = mask_pii_in_dataframe_with_audit(df)
    except ValueError as exc:
        # Empty or invalid DataFrame
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to mask PII: {exc}")

    # Build markdown preview from headers and first 3 rows
    try:
        preview_df = masked_df.head(3)
        markdown_preview = preview_df.to_markdown(index=False)
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to build markdown preview: {exc}",
        )

    try:
        mapping = _map_headers_with_llama_3_1(markdown_preview)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"LLM mapping failed: {exc}")

    # Record LLM mapping decisions for auditability.
    for target_field in ("date", "amount", "description", "transaction_type"):
        original_col = mapping.get(target_field)
        audit_trail.append(
            {
                "action": "header_mapping",
                "original_col": original_col,
                "mapped_to": target_field,
                "reason": "Semantic match",
            }
        )

    # Apply mapping to DataFrame columns.
    # Expected shape: {"date": "orig_date_col", "amount": "orig_amount_col", ...}
    rename_map: dict[str, str] = {}
    for target_field in ("date", "amount", "description", "transaction_type"):
        source_name = mapping.get(target_field)
        if isinstance(source_name, str) and source_name in masked_df.columns:
            rename_map[source_name] = target_field

    standardized_df = masked_df.rename(columns=rename_map)

    # Ensure all standard columns exist; fill missing ones.
    if "transaction_type" not in standardized_df.columns:
        standardized_df["transaction_type"] = "N/A"
    if "date" not in standardized_df.columns:
        standardized_df["date"] = None
    if "amount" not in standardized_df.columns:
        standardized_df["amount"] = None
    if "description" not in standardized_df.columns:
        standardized_df["description"] = None

    # --- Value normalization ---
    # 1) date -> YYYY-MM-DD (string), robust to mixed formats
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
        raise HTTPException(status_code=500, detail=f"Failed to normalize date: {exc}")

    # 2) amount -> float (strip currency symbols, commas, plus signs)
    try:
        amount_raw = standardized_df["amount"].astype("string")
        amount_clean = amount_raw.str.replace(r"[^\d\.\-]", "", regex=True)
        amount_num = pd.to_numeric(amount_clean, errors="coerce")
        standardized_df["amount"] = amount_num
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to normalize amount: {exc}")

    # 3) transaction_type -> credit/debit based on sign of amount
    try:
        standardized_df["transaction_type"] = np.where(
            standardized_df["amount"].isna(),
            "N/A",
            np.where(standardized_df["amount"] >= 0, "credit", "debit"),
        )
    except Exception as exc:
        raise HTTPException(
            status_code=500, detail=f"Failed to infer transaction_type: {exc}"
        )

    # Reorder and filter to the canonical schema
    standardized_df = standardized_df[
        ["date", "amount", "description", "transaction_type"]
    ]

    # Convert to list-of-dicts for the API response
    standardized_df = standardized_df.replace({np.nan: None})
    records = standardized_df.to_dict(orient="records")
    return {"mapped_data": records, "audit_trail": audit_trail}


