from __future__ import annotations

import re
from typing import Any

import pandas as pd
from pandas import DataFrame


ACCOUNT_NUMBER_PATTERN = re.compile(r"\b\d{10,12}\b")
EMAIL_PATTERN = re.compile(
    r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b",
    re.IGNORECASE,
)


def _mask_text(value: str) -> str:
    """Mask account numbers and emails in a single string."""
    # Mask 10–12 digit account numbers
    masked = ACCOUNT_NUMBER_PATTERN.sub("--XXXX", value)
    # Mask email addresses
    masked = EMAIL_PATTERN.sub("--XXXX", masked)
    return masked


def mask_pii_in_dataframe(df: DataFrame) -> DataFrame:
    """
    Find and mask PII in a Pandas DataFrame.

    - 10–12 digit bank account numbers are replaced with ``"--XXXX"``.
    - Email addresses are replaced with ``"--XXXX"``.

    Parameters
    ----------
    df:
        Input Pandas DataFrame.

    Returns
    -------
    DataFrame
        A new DataFrame with PII masked.

    Raises
    ------
    TypeError
        If the input is not a Pandas DataFrame.
    ValueError
        If the DataFrame is empty.
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("mask_pii_in_dataframe expects a pandas.DataFrame instance.")

    if df.empty:
        raise ValueError("Cannot mask PII on an empty DataFrame.")

    cleaned = df.copy(deep=True)

    try:
        string_like_columns = cleaned.select_dtypes(include=["object", "string"]).columns

        for col in string_like_columns:
            cleaned[col] = cleaned[col].apply(
                lambda v: _mask_text(v) if isinstance(v, str) and v else v
            )

        return cleaned
    except Exception as exc:  # pragma: no cover - defensive programming
        raise RuntimeError("Failed to mask PII in DataFrame.") from exc

