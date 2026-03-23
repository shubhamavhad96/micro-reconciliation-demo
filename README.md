# Micro-Reconciliation Agent

An AI-powered, production-grade ingestion engine for mapping messy bank statements to canonical financial ledgers. Built with a strict focus on data security, SOC2 compliance, and high-throughput reliability.

## The Architecture & "Why"

Most AI wrappers fail in fintech because they pipe raw financial data to an LLM, causing massive token costs, latency timeouts, and severe data privacy violations. This system was architected to solve those specific bottlenecks:

### 1. Zero-Leakage PII Masking

Financial data should never touch a 3rd-party LLM. The ingestion pipeline runs a local, deterministic Regex Engine to scrub Emails, Account Numbers, and SSNs before constructing the LLM prompt.

### 2. O(1) Token Cost via Schema Sampling

Sending a 100,000-row CSV to an LLM will hang the system and drain API credits. This engine uses **Schema Sampling**:

- It extracts only the first 3 to 10 rows.
- It sends this micro-sample to Llama 3.1 to deduce the header mapping logic.
- It passes the resulting JSON map to **Pandas**, which executes the transformation across the remaining 99,990 rows locally in milliseconds.

### 3. Human-in-the-Loop (HITL) Safety Net

AI hallucinates. If the LLM's confidence score for a column mapping drops below 95%, the system refuses to auto-approve it. It flags the column in the UI (Mapping Status: Needs Review or Manual) to guarantee zero silent ledger corruption.

### 4. Vectorized "Fuzzy" Fee Detection (Stripe)

Ledgers rarely match 1:1 because of hidden payment gateway fees. The Pandas layer runs a vectorized forward/backward calculation to detect implied Stripe payouts (2.9% + $0.30). It surfaces a 1-click **Add Stripe Fee** resolution in the UI to instantly fix broken reconciliations.

### 5. Asynchronous Event Queue

To handle massive enterprise CSVs without UI freezing or HTTP 504 timeouts, the entire ingestion pipeline is wrapped in a FastAPI **BackgroundTask** queue, allowing the Next.js client to gracefully poll for progress.

## Tech Stack & Structure

Strictly separated full-stack monorepo:

- **frontend/**: Next.js 14 + Tailwind v4 + react-dropzone
- **backend/**: FastAPI + Pandas + Llama 3.1

---

## How to Run Locally

Built to spin up in under 30 seconds. No external Redis/Celery dependencies required for the async queue (uses an in-memory task store for easy local testing).

### 1. Backend (FastAPI)

```bash
cd backend
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```

Health check: http://localhost:8000/health

### 2. Frontend (Next.js)

```bash
cd frontend
npm install
npm run dev
```

Open: http://localhost:3000 to view the client.

### Optional: Point frontend to a different API

Set in `frontend/.env.local`:

```bash
NEXT_PUBLIC_API_BASE=https://your-api.example.com
```
