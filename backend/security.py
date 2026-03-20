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
    """
    Deterministically redact sensitive substrings from free-form text.

    For SOC2-style controls, we prefer regex *false positives* over data
    leakage. Masking too much is safer than masking too little when
    preparing content for an external system.

    The stable replacement token (``"--XXXX"``) keeps parsing predictable
    while avoiding retention of sensitive values in prompts, logs, or audit
    output.

    Parameters
    ----------
    value:
        Free-form text that may contain sensitive substrings.

    Returns
    -------
    str
        Redacted text using deterministic replacement tokens.
    """
    masked = ACCOUNT_NUMBER_PATTERN.sub("--XXXX", value)
    # Mask email addresses
    masked = EMAIL_PATTERN.sub("--XXXX", masked)
    return masked


def mask_pii_in_dataframe(df: DataFrame) -> DataFrame:
    """
    Mask PII locally in a DataFrame using deterministic regex redaction.

    This wrapper exists for backward compatibility: it performs the local
    redaction step without producing an audit trail.

    Masking happens *before* any LLM call so regulated values are never sent
    to an external model. This supports SOC2-style data-handling
    requirements.

    Parameters
    ----------
    df:
        Input DataFrame produced from the uploaded CSV.

    Returns
    -------
    DataFrame
        A deep-copied DataFrame with sensitive substrings replaced.

    Raises
    ------
    TypeError
        If ``df`` is not a Pandas DataFrame.
    ValueError
        If ``df`` is empty (masking would be a no-op and hides input problems).
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("mask_pii_in_dataframe expects a pandas.DataFrame instance.")

    if df.empty:
        raise ValueError("Cannot mask PII on an empty DataFrame.")

    cleaned, _ = mask_pii_in_dataframe_with_audit(df)
    return cleaned


def mask_pii_in_dataframe_with_audit(
    df: DataFrame,
) -> tuple[DataFrame, list[dict[str, Any]]]:
    """
    Mask PII locally and return a lightweight, non-sensitive audit trail.

    Masking occurs locally *before* the LLM prompt is constructed. This
    reduces the risk of data leakage in prompts and logs, which supports
    SOC2-style handling requirements.

    The audit trail is intentionally non-sensitive: it records detection
    counts (not raw matches) so reviewers can validate that redaction was
    attempted without creating sensitive-data retention.

    Parameters
    ----------
    df:
        Input DataFrame produced from the uploaded CSV.

    Returns
    -------
    tuple[DataFrame, list[dict[str, Any]]]
        The masked DataFrame and a non-sensitive audit trail describing
        redaction attempts via local regex detection counts.
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("mask_pii_in_dataframe_with_audit expects a pandas.DataFrame instance.")

    if df.empty:
        raise ValueError("Cannot mask PII on an empty DataFrame.")

    cleaned = df.copy(deep=True)

    try:
        string_like_columns = cleaned.select_dtypes(include=["object", "string"]).columns

        # Count regex hits before masking (best-effort).
        account_count = 0
        email_count = 0
        for col in string_like_columns:
            series = cleaned[col].dropna()
            if series.empty:
                continue
            as_str = series.astype(str)
            account_count += int(as_str.str.findall(ACCOUNT_NUMBER_PATTERN).apply(len).sum())
            email_count += _email_match_count(as_str)

        audit_trail: list[dict[str, Any]] = [
            {
                "action": "pii_masking",
                "target": "account_number",
                "method": "local_regex",
                "count": account_count,
            },
            {
                "action": "pii_masking",
                "target": "email_address",
                "method": "local_regex",
                "count": email_count,
            },
        ]

        # Apply masking
        for col in string_like_columns:
            cleaned[col] = cleaned[col].apply(
                lambda v: _mask_text(v) if isinstance(v, str) and v else v
            )

        return cleaned, audit_trail
    except Exception as exc:  # pragma: no cover - defensive programming
        raise RuntimeError("Failed to mask PII in DataFrame.") from exc


def _email_match_count(as_str: pd.Series) -> int:
    """Sum email regex match counts across a Series for audit transparency."""
    return int(as_str.str.findall(EMAIL_PATTERN).apply(len).sum())

