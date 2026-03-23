from __future__ import annotations

import re
from typing import Any

import numpy as np
import pandas as pd
from pandas import DataFrame, Index


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


def sample_masked_dataframe_for_llm(masked_df: DataFrame, max_rows: int) -> DataFrame:
    """Bound the LLM prompt to a fixed row cap for schema-only inference.

    Schema sampling decouples prompt size from file size: the markdown table sent
    to Llama 3.1 includes only ``head(max_rows)``, so token count and request
    latency stay bounded even when the on-disk CSV has millions of rows. Full
    rows are still processed later in ``main.py`` with pandas; only the LLM
    call is constrained.

    Security: ``masked_df`` must already be PII-masked; this function does not
    change cell values, only row count.

    Args:
        masked_df: Masked dataframe (same columns as upload; no raw PII in cells).
        max_rows: Max data rows in the sample (table header is additional).

    Returns:
        At most ``max_rows`` rows from the top of ``masked_df``, or fewer if
        shorter.

    Raises:
        ValueError: If ``max_rows`` < 1.

    Note:
        ``max_rows`` is chosen in ``main.py`` (e.g. 10 vs 3 for large uploads) to
        keep the Groq/Llama request within a predictable token budget and avoid
        provider timeouts on huge files.
    """
    if max_rows < 1:
        raise ValueError("max_rows must be >= 1 for LLM schema sampling.")
    return masked_df.head(max_rows)


# Canonical ledger fields used by the reconciliation API and HITL prompts.
CANONICAL_LEDGER_FIELDS: tuple[str, ...] = (
    "date",
    "amount",
    "description",
    "transaction_type",
)


def hitl_status_from_confidence_score(confidence_score: int) -> str:
    """Map a 0–100 score to HITL status labels used by API and audit.

    Thresholds are fixed and applied **after** parsing LLM output so labels stay
    aligned with numeric scores even when the model emits inconsistent ``status``
    text.

    Args:
        confidence_score: Raw score; non-integers and out-of-range values are
            clamped to 0–100. Then: ``> 95`` → ``auto_approved``;
            ``70`` ≤ score ≤ ``95`` → ``needs_review``; ``< 70`` → ``rejected``.

    Returns:
        One of ``auto_approved``, ``needs_review``, ``rejected``.
    """
    try:
        s = int(confidence_score)
    except (TypeError, ValueError):
        s = 0
    s = max(0, min(100, s))
    if s > 95:
        return "auto_approved"
    if s >= 70:
        return "needs_review"
    return "rejected"


def hitl_header_mapping_system_prompt() -> str:
    """Build the system prompt string for Groq/Llama header mapping.

    Instructs the model to emit **only** JSON: per canonical field, either
    ``null`` or ``{source_column, confidence_score, status}``. Downstream code
    does **not** treat ``status`` as authoritative—see
    :func:`normalize_hitl_llm_mapping`.

    Returns:
        Prompt text concatenated for ``ChatGroq`` system role.

    Note:
        Payload still depends on prior PII masking; never call with raw CSV cells.
    """
    fields = ", ".join(f'"{f}"' for f in CANONICAL_LEDGER_FIELDS)
    return (
        "You are a financial data normalization assistant. "
        "You are given a markdown table (CSV headers + masked sample rows). "
        "Map each messy CSV column to at most one of these canonical ledger fields: "
        f"{fields}.\n\n"
        "Return **only** a single JSON object. Each key must be one of those field names. "
        "Each value must be either:\n"
        '- `null` if there is no suitable column, or\n'
        "- an object with exactly these keys:\n"
        '  - `"source_column"`: string, the exact CSV header name from the table, or null\n'
        '  - `"confidence_score"`: integer from 0 to 100 (semantic certainty)\n'
        '  - `"status"`: one of `"auto_approved"`, `"needs_review"`, `"rejected"` '
        "using **these strict rules**:\n"
        "    - If confidence_score > 95 → status MUST be `auto_approved`\n"
        "    - If 70 ≤ confidence_score ≤ 95 → status MUST be `needs_review`\n"
        "    - If confidence_score < 70 → status MUST be `rejected`\n\n"
        "Example shape:\n"
        "{\n"
        '  "date": {"source_column": "Txn Date", "confidence_score": 98, "status": "auto_approved"},\n'
        '  "amount": {"source_column": "Amt", "confidence_score": 82, "status": "needs_review"},\n'
        '  "description": null,\n'
        '  "transaction_type": {"source_column": "Type", "confidence_score": 40, "status": "rejected"}\n'
        "}\n\n"
        "Do not include markdown fences, comments, or any text outside the JSON object."
    )


def normalize_hitl_llm_mapping(
    raw: dict[str, Any],
    valid_columns: Index,
) -> tuple[dict[str, str | None], dict[str, dict[str, Any]]]:
    """Normalize LLM header-mapping JSON into auditable, enforceable HITL metadata.

    The model may emit wrong ``source_column`` names, out-of-range scores, or
    ``status`` labels that disagree with its own ``confidence_score``. This
    function never trusts the LLM's ``status`` field: after clamping scores to
    0–100, :func:`hitl_status_from_confidence_score` recomputes ``status`` so
    audit/UI thresholds stay consistent. ``source_column`` is accepted only if
    it matches a real index label in ``valid_columns``; otherwise the field is
    treated as unmapped (score forced to 0, ``rejected``). Legacy flat string
    values are mapped conservatively when the name exists.

    Args:
        raw: Parsed JSON object from the LLM (keys = canonical fields).
        valid_columns: Actual column index from the uploaded dataframe (ground
            truth for what exists).

    Returns:
        A pair ``(field_sources, hitl)`` where ``field_sources`` maps each
        canonical key to the chosen source column name or ``None``, and ``hitl``
        holds ``source_column``, ``confidence_score``, and recomputed ``status``
        per field.

    Note:
        Hallucinated column names cannot rename dataframe columns downstream;
        they only affect metadata until ``valid_columns`` rejects them.
    """
    sources: dict[str, str | None] = {}
    hitl: dict[str, dict[str, Any]] = {}

    for field in CANONICAL_LEDGER_FIELDS:
        val = raw.get(field)

        if isinstance(val, str):
            sc = val if val in valid_columns else None
            conf = 50 if sc is not None else 0
            st = hitl_status_from_confidence_score(conf)
            hitl[field] = {
                "source_column": sc,
                "confidence_score": conf,
                "status": st,
            }
            sources[field] = sc
            continue

        if val is None:
            hitl[field] = {
                "source_column": None,
                "confidence_score": 0,
                "status": "rejected",
            }
            sources[field] = None
            continue

        if not isinstance(val, dict):
            hitl[field] = {
                "source_column": None,
                "confidence_score": 0,
                "status": "rejected",
            }
            sources[field] = None
            continue

        sc_raw = val.get("source_column")
        if sc_raw is None:
            sc_raw = val.get("column")
        sc: str | None
        if sc_raw is None:
            sc = None
        elif isinstance(sc_raw, str):
            sc = sc_raw if sc_raw in valid_columns else None
        else:
            sc = str(sc_raw) if str(sc_raw) in valid_columns else None

        try:
            conf = int(val.get("confidence_score", 0))
        except (TypeError, ValueError):
            conf = 0
        conf = max(0, min(100, conf))
        if sc is None:
            conf = 0

        st = hitl_status_from_confidence_score(conf)
        hitl[field] = {
            "source_column": sc,
            "confidence_score": conf,
            "status": st,
        }
        sources[field] = sc

    return sources, hitl


# Stripe-style US card pricing (2.9% + $0.30). Used only for deterministic math — no LLM.
STRIPE_FEE_RATE = 0.029
STRIPE_FIXED_FEE_USD = 0.30


def add_stripe_fee_fuzzy_fields(df: DataFrame) -> DataFrame:
    """Stripe card-pricing heuristic on canonical rows (2.9% + \\$0.30); no LLM.

    Business model: treat ``amount`` as a candidate **net** after fees, infer
    implied gross ``g = (amount + fixed) / (1 - rate)`` and fee ``g - amount``.
    All scoring is **vectorized** (pandas Series + NumPy arrays): there is no
    Python ``for`` over rows; runtime scales linearly with ``len(df)`` with
    fixed work per element in the vector ops.

    **Identity check (float):** require ``abs(rate * g + fixed - (g - amount)) < 0.05``.
    In exact arithmetic those quantities are equal for any positive ``amount``;
    the **\\$0.05** band exists only to absorb IEEE-754 drift, not to classify
    business intent. It does **not** stop “random” credits from satisfying the
    algebra.

    **Smart gates (at least one required for ``has_fee_discrepancy``):**
    substring match on ``description`` for ``stripe`` / ``payout``, or implied
    gross within \\$0.05 of a whole dollar. Those filters reduce false positives
    on typical wire/invoice lines that are positive but not Stripe net payouts.

    ``suggested_gross`` / ``detected_fee`` are filled for valid positive amounts
    for UI math; ``has_fee_discrepancy`` is the stricter boolean.

    Args:
        df: Canonical dataframe with numeric ``amount``; ``description`` used
            only for keyword gating when present.

    Returns:
        Copy of ``df`` with ``has_fee_discrepancy``, ``suggested_gross``,
        ``detected_fee`` added.

    Note:
        Does not read secrets or call external services; safe to run on masked
        data already in-process.
    """
    out = df.copy()
    if "amount" not in out.columns:
        out["has_fee_discrepancy"] = False
        out["suggested_gross"] = np.nan
        out["detected_fee"] = np.nan
        return out

    amt = pd.to_numeric(out["amount"], errors="coerce")
    rate = STRIPE_FEE_RATE
    fixed = STRIPE_FIXED_FEE_USD
    denom = 1.0 - rate

    gross = (amt + fixed) / denom
    fee = gross - amt
    expected_fee = rate * gross + fixed

    a = amt.to_numpy(dtype=float, copy=False)
    g = gross.to_numpy(dtype=float, copy=False)
    f = fee.to_numpy(dtype=float, copy=False)
    ef = expected_fee.to_numpy(dtype=float, copy=False)

    valid = np.isfinite(a) & (a > 0)
    # Forward vs reverse fee should match; allow small float drift.
    model_ok = np.isfinite(g) & np.isfinite(f) & np.isfinite(ef) & (np.abs(ef - f) < 0.05)
    base_ok = valid & model_ok & (f >= 0.01) & (a >= 0.01)

    # Smart condition 1: description mentions Stripe / payout (canonical column).
    n = len(out)
    if "description" in out.columns:
        desc_lower = out["description"].astype(str).str.lower()
        keyword_ok = (
            desc_lower.str.contains("stripe", regex=False, na=False)
            | desc_lower.str.contains("payout", regex=False, na=False)
        ).to_numpy(dtype=bool, copy=False)
    else:
        keyword_ok = np.zeros(n, dtype=bool)

    # Smart condition 2: implied gross sits within $0.05 of a whole dollar.
    clean_gross_ok = np.isfinite(g) & (np.abs(g - np.round(g)) < 0.05)

    smart_ok = keyword_ok | clean_gross_ok
    has_disc = base_ok & smart_ok

    out["suggested_gross"] = np.where(valid, g, np.nan)
    out["detected_fee"] = np.where(valid, f, np.nan)
    out["has_fee_discrepancy"] = has_disc
    return out

