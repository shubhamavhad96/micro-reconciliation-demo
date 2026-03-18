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

from security import mask_pii_in_dataframe


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


@app.post("/reconcile")
async def reconcile(file: UploadFile = File(...)) -> list[dict[str, Any]]:
    """
    Accept a CSV upload, mask PII, build a markdown preview, and
    ask ChatGroq (via LangChain) to map the headers to the canonical schema.
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

    # Mask PII using shared helper
    try:
        masked_df = mask_pii_in_dataframe(df)
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

    # Compose LLM prompt
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

    # Call ChatGroq via LangChain (llama-3.1-8b-instant)
    try:
        llm = ChatGroq(model="llama-3.1-8b-instant")
        response = llm.invoke(
            [
                ("system", system_instruction),
                ("user", user_message),
            ]
        )
        content = getattr(response, "content", None)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"LLM call failed: {exc}")

    if not content:
        raise HTTPException(status_code=500, detail="Empty response from LLM.")

    # Parse LLM output as JSON
    try:
        mapping = json.loads(content)
    except json.JSONDecodeError as exc:
        raise HTTPException(
            status_code=500,
            detail=f"LLM response was not valid JSON: {exc}: {content!r}",
        )

    if not isinstance(mapping, dict):
        raise HTTPException(
            status_code=500,
            detail=f"LLM response JSON was not an object: {mapping!r}",
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
    return records


