 "use client";

import { useCallback, useState } from "react";
import { useDropzone } from "react-dropzone";
import { ShieldCheck, UploadCloud } from "lucide-react";

type MappingResponse = {
  date: string | null;
  amount: number | null;
  description: string | null;
  transaction_type: string | null;
};

type AuditEvent = {
  action: string;
  [key: string]: unknown;
};

type UiState = "idle" | "loading" | "done" | "error";

export default function Home() {
  const [uiState, setUiState] = useState<UiState>("idle");
  const [error, setError] = useState<string | null>(null);
  const [rows, setRows] = useState<MappingResponse[]>([]);
  const [auditTrail, setAuditTrail] = useState<AuditEvent[]>([]);
  const [showLogs, setShowLogs] = useState(false);
  const [fileName, setFileName] = useState<string | null>(null);

  const onDrop = useCallback(async (acceptedFiles: File[]) => {
    const file = acceptedFiles[0];
    if (!file) return;

    setFileName(file.name);
    setError(null);
    setUiState("loading");
    setRows([]);
    setAuditTrail([]);
    setShowLogs(false);

    try {
      const form = new FormData();
      form.append("file", file);

      const res = await fetch("http://localhost:8000/reconcile", {
        method: "POST",
        body: form,
      });

      const json = (await res.json()) as
        | MappingResponse[]
        | { data: MappingResponse[] }
        | { mapped_data: MappingResponse[]; audit_trail: AuditEvent[] }
        | { detail?: string };

      if (!res.ok) {
        const message =
          typeof json === "object" && json && "detail" in json && json.detail
            ? String(json.detail)
            : "Failed to reconcile file.";
        setError(message);
        setUiState("error");
        return;
      }

      let records: MappingResponse[] | null = null;
      let audit: AuditEvent[] = [];

      if (Array.isArray(json)) {
        records = json;
      } else if (typeof json === "object" && json !== null) {
        const obj = json as Record<string, unknown>;
        const dataVal = obj["data"];
        const mappedVal = obj["mapped_data"];

        if (Array.isArray(dataVal)) {
          records = dataVal as MappingResponse[];
        } else if (Array.isArray(mappedVal)) {
          records = mappedVal as MappingResponse[];
          const auditVal = obj["audit_trail"];
          if (Array.isArray(auditVal)) {
            audit = auditVal as AuditEvent[];
          }
        }
      }

      if (!records) {
        setError("Unexpected response shape from /reconcile.");
        setUiState("error");
        return;
      }

      setRows(records);
      setAuditTrail(audit);
      setShowLogs(audit.length > 0);
      setUiState("done");
    } catch (e) {
      setError(e instanceof Error ? e.message : "Unexpected error.");
      setUiState("error");
    }
  }, []);

  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop,
    multiple: false,
    accept: { "text/csv": [".csv"] },
    maxFiles: 1,
  });

  const showTable = uiState === "done";

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

        <section className="mx-auto flex w-full max-w-5xl flex-col gap-6">
          <div
            {...getRootProps({
              className:
                "w-full flex cursor-pointer flex-col items-center justify-center rounded border border-dashed border-zinc-700 bg-zinc-950 px-6 py-10 text-center text-sm text-zinc-400 transition-colors hover:border-zinc-500",
            })}
          >
            <input {...getInputProps()} />
            <UploadCloud className="mb-3 h-6 w-6 text-zinc-500" aria-hidden />
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

          {uiState === "loading" && (
            <div className="flex items-center justify-between rounded border border-zinc-800 bg-zinc-950 px-3 py-2 text-xs text-zinc-400">
              <span className="animate-pulse">
                Agent mapping schemas and masking PII...
              </span>
              <span className="text-[10px] uppercase tracking-wide text-zinc-600">
                Calling /reconcile
              </span>
            </div>
          )}

          {uiState === "error" && error && (
            <div className="rounded border border-red-900 bg-red-950/40 px-3 py-2 text-xs text-red-200">
              {error}
            </div>
          )}

          {showTable && (
            <div className="grid items-start gap-4 lg:grid-cols-[70%_30%]">
              <div className={`w-full ${showLogs ? "" : "lg:col-span-2"}`}>
                <div className="mb-4 flex w-full items-center justify-between gap-3">
                  <div className="text-[11px] font-medium uppercase tracking-wide text-zinc-500">
                    MAPPED DATA
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
                          <td className="px-3 py-2 text-right font-mono">
                            {item.amount ?? "—"}
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
              </div>

              {showLogs && auditTrail.length > 0 ? (
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
                                {String(
                                  (ev as Record<string, unknown>)["reason"],
                                )}
                                <br />
                              </>
                            ) : null}
                            {"target" in ev ? (
                              <>
                                target:{" "}
                                {String(
                                  (ev as Record<string, unknown>)["target"],
                                )}
                                <br />
                              </>
                            ) : null}
                            {"method" in ev ? (
                              <>
                                method:{" "}
                                {String(
                                  (ev as Record<string, unknown>)["method"],
                                )}
                                <br />
                              </>
                            ) : null}
                            {"count" in ev ? (
                              <>
                                count:{" "}
                                {String(
                                  (ev as Record<string, unknown>)["count"],
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
            </div>
          )}
        </section>
      </main>
    </div>
  );
}
