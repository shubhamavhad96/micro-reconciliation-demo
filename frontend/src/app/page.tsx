"use client";

import { useCallback, useEffect, useState } from "react";
import { useDropzone } from "react-dropzone";
import {
  AlertCircle,
  AlertTriangle,
  CheckCircle2,
  ShieldCheck,
  UploadCloud,
} from "lucide-react";

type MappingResponse = {
  date: string | null;
  amount: number | null;
  description: string | null;
  transaction_type: string | null;
  has_fee_discrepancy?: boolean | null;
  suggested_gross?: number | null;
  detected_fee?: number | null;
};

type HitlMappingStatus = "auto_approved" | "needs_review" | "rejected";

type HitlFieldMeta = {
  source_column: string | null;
  confidence_score: number;
  status: HitlMappingStatus | string;
};

const HITL_FIELD_LABELS: Record<string, string> = {
  date: "Date",
  amount: "Amount",
  description: "Description",
  transaction_type: "Type",
};

type AuditEvent = {
  action: string;
  [key: string]: unknown;
};

type UiState = "idle" | "uploading" | "processing" | "done" | "error";

const API_BASE = "http://localhost:8000";

export default function Home() {
  const [uiState, setUiState] = useState<UiState>("idle");
  const [error, setError] = useState<string | null>(null);
  const [rows, setRows] = useState<MappingResponse[]>([]);
  const [auditTrail, setAuditTrail] = useState<AuditEvent[]>([]);
  const [showLogs, setShowLogs] = useState(false);
  const [fileName, setFileName] = useState<string | null>(null);
  const [totalRowCount, setTotalRowCount] = useState<number | null>(null);
  const [hitlMapping, setHitlMapping] = useState<Record<
    string,
    HitlFieldMeta
  > | null>(null);
  const [taskId, setTaskId] = useState<string | null>(null);
  const [jobProgress, setJobProgress] = useState(0);

  const applyReconcilePayload = useCallback(
    (obj: Record<string, unknown>) => {
      let records: MappingResponse[] | null = null;
      let audit: AuditEvent[] = [];
      let totalRows: number | null = null;
      let hitl: Record<string, HitlFieldMeta> | null = null;

      const dataVal = obj["data"];
      const mappedVal = obj["mapped_data"];
      const totalVal = obj["total_rows"];
      const hitlVal = obj["hitl_mapping"];

      if (typeof totalVal === "number" && Number.isFinite(totalVal)) {
        totalRows = totalVal;
      }

      if (
        hitlVal &&
        typeof hitlVal === "object" &&
        hitlVal !== null &&
        !Array.isArray(hitlVal)
      ) {
        hitl = hitlVal as Record<string, HitlFieldMeta>;
      }

      if (Array.isArray(dataVal)) {
        records = dataVal as MappingResponse[];
        if (totalRows === null) totalRows = records.length;
      } else if (Array.isArray(mappedVal)) {
        records = mappedVal as MappingResponse[];
        if (totalRows === null) totalRows = records.length;
        const auditVal = obj["audit_trail"];
        if (Array.isArray(auditVal)) {
          audit = auditVal as AuditEvent[];
        }
      }

      if (!records) return false;
      setRows(records);
      setTotalRowCount(totalRows);
      setHitlMapping(hitl);
      setAuditTrail(audit);
      setShowLogs(audit.length > 0);
      return true;
    },
    [],
  );

  useEffect(() => {
    if (!taskId) return;
    let active = true;

    const poll = async () => {
      if (!active) return;
      try {
        const res = await fetch(`${API_BASE}/api/status/${taskId}`);
        if (!active) return;

        if (res.status === 404) {
          setError("Task not found. Try uploading again.");
          setTaskId(null);
          setUiState("error");
          return;
        }

        const data = (await res.json()) as {
          status?: string;
          progress?: number;
          result?: Record<string, unknown> | null;
          error?: string | null;
          detail?: string;
        };

        if (!active) return;

        if (typeof data.progress === "number" && Number.isFinite(data.progress)) {
          setJobProgress(Math.min(100, Math.max(0, data.progress)));
        }

        if (data.status === "completed" && data.result) {
          active = false;
          setTaskId(null);
          const ok = applyReconcilePayload(data.result);
          if (!ok) {
            setError("Unexpected result shape from server.");
            setUiState("error");
            return;
          }
          setUiState("done");
          setJobProgress(100);
          return;
        }

        if (data.status === "failed") {
          active = false;
          setTaskId(null);
          setError(
            data.error != null && String(data.error).trim() !== ""
              ? String(data.error)
              : "Reconciliation failed.",
          );
          setUiState("error");
          setJobProgress(0);
        }
      } catch (e) {
        if (!active) return;
        setTaskId(null);
        setError(e instanceof Error ? e.message : "Polling failed.");
        setUiState("error");
      }
    };

    const id = window.setInterval(() => {
      void poll();
    }, 500);
    void poll();
    return () => {
      active = false;
      window.clearInterval(id);
    };
  }, [taskId, applyReconcilePayload]);

  const onDrop = useCallback(async (acceptedFiles: File[]) => {
    const file = acceptedFiles[0];
    if (!file) return;

    setFileName(file.name);
    setError(null);
    setUiState("uploading");
    setRows([]);
    setAuditTrail([]);
    setTotalRowCount(null);
    setHitlMapping(null);
    setShowLogs(false);
    setTaskId(null);
    setJobProgress(0);

    try {
      const form = new FormData();
      form.append("file", file);

      const res = await fetch(`${API_BASE}/reconcile`, {
        method: "POST",
        body: form,
      });

      const json = (await res.json()) as
        | { task_id?: string; status?: string; detail?: string }
        | { detail?: string };

      if (res.status === 202) {
        const tid =
          typeof json === "object" && json && "task_id" in json
            ? String((json as { task_id?: string }).task_id ?? "")
            : "";
        if (!tid) {
          setError("No task_id in 202 response.");
          setUiState("error");
          return;
        }
        setTaskId(tid);
        setUiState("processing");
        setJobProgress(0);
        return;
      }

      if (!res.ok) {
        const message =
          typeof json === "object" && json && "detail" in json && json.detail
            ? String(json.detail)
            : "Failed to start reconciliation.";
        setError(message);
        setUiState("error");
        return;
      }

      setError("Unexpected response from server (expected 202).");
      setUiState("error");
    } catch (e) {
      setError(e instanceof Error ? e.message : "Unexpected error.");
      setUiState("error");
    }
  }, []);

  const applyStripeFee = useCallback((idx: number) => {
    setRows((prev) => {
      const row = prev[idx];
      if (
        !row?.has_fee_discrepancy ||
        row.suggested_gross == null ||
        row.detected_fee == null ||
        row.amount == null
      ) {
        return prev;
      }
      const originalNet = row.amount;
      const feeAdded = row.detected_fee;
      const newGross = row.suggested_gross;
      queueMicrotask(() => {
        setAuditTrail((a) => [
          ...a,
          {
            action: "fee_reconciliation_applied",
            original_net_amount: originalNet,
            detected_fee_added: feeAdded,
            new_gross_amount: newGross,
            row_index: idx,
          },
        ]);
        setShowLogs(true);
      });
      return prev.map((r, i) =>
        i === idx
          ? {
              ...r,
              amount: newGross,
              has_fee_discrepancy: false,
              suggested_gross: null,
              detected_fee: null,
            }
          : r,
      );
    });
  }, []);

  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop,
    multiple: false,
    accept: { "text/csv": [".csv"] },
    maxFiles: 1,
  });

  const showTable = uiState === "done";
  const showProgressPanel =
    uiState === "uploading" || uiState === "processing";

  const formatUsd = useCallback((n: number | null | undefined) => {
    if (n == null || Number.isNaN(Number(n))) return "—";
    return new Intl.NumberFormat("en-US", {
      style: "currency",
      currency: "USD",
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    }).format(Number(n));
  }, []);

  function hitlStatusChip(fieldKey: string, meta: HitlFieldMeta) {
    const label = HITL_FIELD_LABELS[fieldKey] ?? fieldKey;
    const st = meta.status as HitlMappingStatus | string;
    const score = meta.confidence_score;
    const src =
      meta.source_column != null && meta.source_column !== ""
        ? meta.source_column
        : "—";

    const base =
      "inline-flex max-w-full items-center gap-1.5 rounded-md border px-2 py-1 text-[10px] font-medium";

    if (st === "auto_approved") {
      return (
        <span
          key={fieldKey}
          className={`${base} border-emerald-900/60 bg-emerald-950/35 text-emerald-300`}
          title={`${label}: ${src} (${score})`}
        >
          <CheckCircle2
            className="h-3.5 w-3.5 shrink-0 text-emerald-400/90"
            aria-hidden
          />
          <span className="truncate">
            {label}{" "}
            <span className="font-normal text-emerald-400/80">· auto</span>
          </span>
        </span>
      );
    }
    if (st === "needs_review") {
      return (
        <span
          key={fieldKey}
          className={`${base} border-amber-500/70 bg-amber-950/55 text-amber-100 shadow-[0_0_0_1px_rgba(245,158,11,0.15)]`}
          title={`${label}: ${src} (${score})`}
        >
          <AlertTriangle
            className="h-3.5 w-3.5 shrink-0 text-amber-400"
            aria-hidden
          />
          <span className="truncate">
            {label}{" "}
            <span className="font-semibold text-amber-200">· review</span>
          </span>
        </span>
      );
    }
    return (
      <span
        key={fieldKey}
        className={`${base} border-red-800/80 bg-red-950/45 text-red-100 shadow-[0_0_0_1px_rgba(248,113,113,0.12)]`}
        title={`${label}: ${src} (${score})`}
      >
        <AlertCircle className="h-3.5 w-3.5 shrink-0 text-red-400" aria-hidden />
        <span className="truncate">
          {label}{" "}
          <span className="font-semibold text-red-200">· manual</span>
        </span>
      </span>
    );
  }

  return (
    <div className="min-h-dvh bg-zinc-950 text-zinc-50">
      <main className="mx-auto flex min-h-dvh max-w-5xl flex-col justify-center px-6 py-10">
        <header className="mb-10 flex items-center justify-between gap-3">
          <div className="flex items-center gap-2">
            <span className="inline-flex h-7 w-7 items-center justify-center rounded bg-zinc-900 text-xs font-medium text-zinc-100">
              MR
            </span>
            <div>
              <h1 className="text-sm font-semibold tracking-tight">
                Micro-Reconciliation
              </h1>
              <p className="text-xs text-zinc-500">
                B2B ledger normalization workspace
              </p>
            </div>
          </div>
          <div className="inline-flex items-center gap-1 rounded-full border border-zinc-800 bg-zinc-950 px-2 py-1 text-[11px] font-medium text-zinc-400">
            <ShieldCheck className="h-3 w-3" aria-hidden />
            <span>Secured</span>
          </div>
        </header>

        <section className="w-full flex flex-col gap-6 lg:grid lg:grid-cols-[70%_30%] lg:gap-4 lg:items-start">
          <div
            className={`flex flex-col gap-6 ${
              showLogs ? "lg:col-span-1" : "lg:col-span-2"
            }`}
          >
            {!showProgressPanel ? (
              <div
                {...getRootProps({
                  className:
                    "w-full flex cursor-pointer flex-col items-center justify-center rounded border border-dashed border-zinc-700 bg-zinc-950 px-6 py-10 text-center text-sm text-zinc-400 transition-colors hover:border-zinc-500",
                })}
              >
                <input {...getInputProps()} />
                <UploadCloud
                  className="mb-3 h-6 w-6 text-zinc-500"
                  aria-hidden
                />
                <p className="font-medium text-zinc-200">
                  {isDragActive ? "Drop to reconcile" : "Drop messy CSV here"}
                </p>
                <p className="mt-1 text-xs text-zinc-500">
                  We will mask PII, sample the schema, and map it to a canonical
                  ledger model.
                </p>
                {fileName ? (
                  <p className="mt-3 text-xs text-zinc-500">
                    Selected: <span className="text-zinc-300">{fileName}</span>
                  </p>
                ) : null}
              </div>
            ) : (
              <div className="w-full overflow-hidden rounded-xl border border-zinc-800 bg-linear-to-br from-zinc-950 via-zinc-950 to-zinc-900 px-6 py-8 shadow-[0_0_0_1px_rgba(255,255,255,0.04)]">
                <div className="flex items-center justify-between gap-3">
                  <div>
                    <p className="text-sm font-semibold text-zinc-100">
                      {uiState === "uploading"
                        ? "Starting job…"
                        : "Reconciling in the background"}
                    </p>
                    <p className="mt-1 text-xs text-zinc-500">
                      {fileName ? (
                        <>
                          <span className="text-zinc-400">{fileName}</span>
                          <span className="text-zinc-600"> · </span>
                        </>
                      ) : null}
                      Schema sampling, HITL mapping, and pandas normalization
                    </p>
                  </div>
                  <div className="tabular-nums text-2xl font-semibold tracking-tight text-violet-300">
                    {Math.round(jobProgress)}%
                  </div>
                </div>
                <div
                  className="mt-5 h-2 w-full overflow-hidden rounded-full bg-zinc-900 ring-1 ring-zinc-800"
                  role="progressbar"
                  aria-valuenow={Math.round(jobProgress)}
                  aria-valuemin={0}
                  aria-valuemax={100}
                >
                  <div
                    className="h-full rounded-full bg-linear-to-r from-violet-600 via-fuchsia-500 to-violet-400 transition-[width] duration-300 ease-out"
                    style={{ width: `${Math.min(100, Math.max(0, jobProgress))}%` }}
                  />
                </div>
                <p className="mt-3 text-[11px] text-zinc-600">
                  Polling task status every 500ms — you can leave this tab open.
                </p>
              </div>
            )}

            {uiState === "error" && error && (
              <div
                className="rounded-lg border border-red-800/80 bg-red-950/90 px-4 py-3 text-xs text-red-100 shadow-lg shadow-red-950/50 ring-1 ring-red-500/20"
                role="alert"
              >
                <p className="font-semibold text-red-200">Reconciliation failed</p>
                <p className="mt-1 whitespace-pre-wrap text-red-200/90">{error}</p>
              </div>
            )}

            {showTable && (
              <>
                <div className="mb-4 flex w-full flex-wrap items-center justify-between gap-3">
                  <div className="flex flex-wrap items-center gap-2">
                    <div className="text-[11px] font-medium uppercase tracking-wide text-zinc-500">
                      MAPPED DATA
                    </div>
                    {totalRowCount !== null && totalRowCount > 100 ? (
                      <span className="rounded-full border border-zinc-700 bg-zinc-900 px-2 py-0.5 text-[10px] font-medium text-zinc-400">
                        Previewing 100 of {totalRowCount} rows.
                      </span>
                    ) : null}
                  </div>

                  {auditTrail.length > 0 && !showLogs ? (
                    <button
                      type="button"
                      onClick={() => setShowLogs(true)}
                      className="rounded border border-transparent bg-white px-3 py-1 text-[11px] font-semibold text-zinc-950 transition-colors hover:bg-zinc-200"
                    >
                      Show Audit Trail
                    </button>
                  ) : null}

                  {auditTrail.length > 0 && showLogs ? (
                    <button
                      type="button"
                      aria-hidden
                      tabIndex={-1}
                      className="invisible rounded border border-zinc-800 bg-zinc-950 px-3 py-1 text-[11px] font-medium"
                    >
                      Collapse
                    </button>
                  ) : null}
                </div>

                {hitlMapping ? (
                  <div className="rounded border border-zinc-800 bg-zinc-900/40 px-3 py-2">
                    <div className="mb-2 text-[10px] font-semibold uppercase tracking-wide text-zinc-500">
                      Mapping status (HITL)
                    </div>
                    <div className="flex flex-wrap gap-2">
                      {Object.entries(hitlMapping).map(([key, meta]) =>
                        hitlStatusChip(key, meta),
                      )}
                    </div>
                  </div>
                ) : null}

                <div
                  className="w-full max-h-[62vh] overflow-y-auto overflow-x-hidden rounded border border-zinc-800 bg-zinc-950
                    [scrollbar-width:thin] [scrollbar-color:#3f3f46_transparent]
                    [&::-webkit-scrollbar]:w-px [&::-webkit-scrollbar-track]:bg-transparent [&::-webkit-scrollbar-thumb]:bg-zinc-800 [&::-webkit-scrollbar-thumb]:rounded-full"
                >
                  <table className="w-full border-collapse text-xs text-zinc-300">
                    <thead className="sticky top-0 z-10 bg-zinc-950">
                      <tr className="border-b border-zinc-800 text-[11px] uppercase tracking-wide text-zinc-500">
                        <th className="px-3 py-2 text-left">Date</th>
                        <th className="px-3 py-2 text-right">Amount</th>
                        <th className="px-3 py-2 text-left">Description</th>
                        <th className="px-3 py-2 text-left">Type</th>
                      </tr>
                    </thead>
                    <tbody>
                      {rows.map((item, idx) => (
                        <tr
                          key={`${item.date ?? "na"}-${item.amount ?? "na"}-${idx}`}
                          className="border-b border-zinc-900/60 hover:bg-zinc-900"
                        >
                          <td className="px-3 py-2 align-top font-mono">
                            {item.date ?? "—"}
                          </td>
                          <td className="px-3 py-2 text-right align-top">
                            <div className="flex flex-col items-end gap-1.5 sm:flex-row sm:items-center sm:justify-end sm:gap-2">
                              <span className="font-mono tabular-nums">
                                {item.amount ?? "—"}
                              </span>
                              {item.has_fee_discrepancy === true &&
                              item.detected_fee != null ? (
                                <button
                                  type="button"
                                  onClick={() => applyStripeFee(idx)}
                                  className="whitespace-nowrap rounded-md border border-violet-500/40 bg-violet-950/50 px-2 py-0.5 text-[10px] font-medium text-violet-200 shadow-sm transition-colors hover:border-violet-400/60 hover:bg-violet-900/50"
                                >
                                  ⚡ Add Stripe Fee (
                                  {formatUsd(item.detected_fee)})
                                </button>
                              ) : null}
                            </div>
                          </td>
                          <td className="px-3 py-2">
                            {item.description ?? "—"}
                          </td>
                          <td className="px-3 py-2">
                            {item.transaction_type ?? "—"}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </>
            )}
          </div>

          {showTable && showLogs && auditTrail.length > 0 ? (
            <aside className="lg:sticky lg:top-6 w-full shrink-0">
              <div className="mb-4 flex w-full items-center justify-between gap-3">
                <div className="flex items-center gap-2">
                  <div className="text-[11px] font-medium uppercase tracking-wide text-zinc-500">
                    Audit Trail
                  </div>
                  <div className="text-[10px] text-zinc-600 whitespace-nowrap">
                    {auditTrail.length} events
                  </div>
                </div>
                <button
                  type="button"
                  onClick={() => setShowLogs(false)}
                  className="rounded border border-zinc-800 bg-zinc-900 px-3 py-1 text-[11px] font-medium text-zinc-300 transition-colors hover:bg-zinc-800"
                >
                  Collapse
                </button>
              </div>

              <div
                className="max-h-[62vh] overflow-y-auto overflow-x-hidden rounded border border-zinc-800 bg-zinc-900 p-3
                  [scrollbar-width:thin] [scrollbar-color:#3f3f46_transparent]
                  [&::-webkit-scrollbar]:w-px [&::-webkit-scrollbar-track]:bg-transparent [&::-webkit-scrollbar-thumb]:bg-zinc-800 [&::-webkit-scrollbar-thumb]:rounded-full"
              >
                <ul className="space-y-2">
                  {auditTrail.map((ev, idx) => (
                    <li
                      key={`${ev.action}-${idx}`}
                      className="rounded border border-zinc-800 bg-zinc-950 px-2 py-2"
                    >
                      <div className="text-[11px] font-semibold text-zinc-200">
                        {ev.action}
                      </div>
                      <div className="mt-1 text-[11px] leading-5 text-zinc-400">
                        {"original_col" in ev ? (
                          <>
                            original_col:{" "}
                            {String(
                              (ev as Record<string, unknown>)["original_col"],
                            )}
                            <br />
                          </>
                        ) : null}
                        {"mapped_to" in ev ? (
                          <>
                            mapped_to:{" "}
                            {String(
                              (ev as Record<string, unknown>)["mapped_to"],
                            )}
                            <br />
                          </>
                        ) : null}
                        {"reason" in ev ? (
                          <>
                            reason:{" "}
                            {String((ev as Record<string, unknown>)["reason"])}
                            <br />
                          </>
                        ) : null}
                        {"target" in ev ? (
                          <>
                            target:{" "}
                            {String((ev as Record<string, unknown>)["target"])}
                            <br />
                          </>
                        ) : null}
                        {"method" in ev ? (
                          <>
                            method:{" "}
                            {String((ev as Record<string, unknown>)["method"])}
                            <br />
                          </>
                        ) : null}
                        {"count" in ev ? (
                          <>
                            count:{" "}
                            {String((ev as Record<string, unknown>)["count"])}
                          </>
                        ) : null}
                        {"lowest_confidence_score" in ev ? (
                          <>
                            lowest_confidence_score:{" "}
                            {String(
                              (ev as Record<string, unknown>)[
                                "lowest_confidence_score"
                              ],
                            )}
                            <br />
                          </>
                        ) : null}
                        {"confidence_score" in ev ? (
                          <>
                            confidence_score:{" "}
                            {String(
                              (ev as Record<string, unknown>)["confidence_score"],
                            )}
                            <br />
                          </>
                        ) : null}
                        {ev.action === "header_mapping" && "status" in ev ? (
                          <>
                            status:{" "}
                            {String((ev as Record<string, unknown>)["status"])}
                            <br />
                          </>
                        ) : null}
                        {ev.action === "fee_reconciliation_applied" &&
                        "original_net_amount" in ev ? (
                          <>
                            original_net_amount:{" "}
                            {String(
                              (ev as Record<string, unknown>)[
                                "original_net_amount"
                              ],
                            )}
                            <br />
                          </>
                        ) : null}
                        {ev.action === "fee_reconciliation_applied" &&
                        "detected_fee_added" in ev ? (
                          <>
                            detected_fee_added:{" "}
                            {String(
                              (ev as Record<string, unknown>)[
                                "detected_fee_added"
                              ],
                            )}
                            <br />
                          </>
                        ) : null}
                        {ev.action === "fee_reconciliation_applied" &&
                        "new_gross_amount" in ev ? (
                          <>
                            new_gross_amount:{" "}
                            {String(
                              (ev as Record<string, unknown>)[
                                "new_gross_amount"
                              ],
                            )}
                            <br />
                          </>
                        ) : null}
                        {ev.action === "fee_reconciliation_applied" &&
                        "row_index" in ev ? (
                          <>
                            row_index:{" "}
                            {String(
                              (ev as Record<string, unknown>)["row_index"],
                            )}
                          </>
                        ) : null}
                      </div>
                    </li>
                  ))}
                </ul>
              </div>
            </aside>
          ) : null}
        </section>
      </main>
    </div>
  );
}
