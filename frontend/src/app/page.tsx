 "use client";

import { useCallback, useState } from "react";
import { useDropzone } from "react-dropzone";
import { ShieldCheck, UploadCloud } from "lucide-react";

type MappingResponse = {
  date: string | null;
  amount: string | null;
  description: string | null;
  transaction_type: string | null;
};

type UiState = "idle" | "loading" | "done" | "error";

export default function Home() {
  const [uiState, setUiState] = useState<UiState>("idle");
  const [error, setError] = useState<string | null>(null);
  const [rows, setRows] = useState<MappingResponse[]>([]);
  const [fileName, setFileName] = useState<string | null>(null);

  const onDrop = useCallback(async (acceptedFiles: File[]) => {
    const file = acceptedFiles[0];
    if (!file) return;

    setFileName(file.name);
    setError(null);
    setUiState("loading");
    setRows([]);

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

      const records = Array.isArray(json)
        ? json
        : typeof json === "object" &&
            json !== null &&
            "data" in json &&
            Array.isArray((json as any).data)
          ? ((json as any).data as MappingResponse[])
          : null;

      if (!records) {
        setError("Unexpected response shape from /reconcile.");
        setUiState("error");
        return;
      }

      setRows(records);
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

  const showTable = uiState === "done" && rows.length > 0;

  return (
    <div className="min-h-dvh bg-zinc-950 text-zinc-50">
      <main className="mx-auto flex min-h-dvh max-w-4xl flex-col justify-center px-6 py-10">
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

        <section className="mx-auto flex w-full max-w-3xl flex-col gap-6">
          <div
            {...getRootProps({
              className:
                "flex cursor-pointer flex-col items-center justify-center rounded border border-dashed border-zinc-700 bg-zinc-950 px-6 py-10 text-center text-sm text-zinc-400 transition-colors hover:border-zinc-500",
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
            <div className="overflow-hidden rounded border border-zinc-800 bg-zinc-950">
              <table className="w-full border-collapse text-xs text-zinc-300">
                <thead className="bg-zinc-950">
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
                      <td className="px-3 py-2">{item.description ?? "—"}</td>
                      <td className="px-3 py-2">
                        {item.transaction_type ?? "—"}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </section>
      </main>
    </div>
  );
}
