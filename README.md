# micro-reconciliation-demo

Strictly separated full-stack monorepo:

- `frontend/`: Next.js 14 + Tailwind + `react-dropzone` (Node environment stays here)
- `backend/`: FastAPI (Python environment stays here)

## Run backend (FastAPI)

```bash
cd backend
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```

Health check: `http://localhost:8000/health`

## Run frontend (Next.js)

```bash
cd frontend
npm install
npm run dev
```

Open: `http://localhost:3000`

## Upload flow

- Use the drag & drop zone to select a `.csv`
- The frontend sends `multipart/form-data` with field name `file` to `POST http://localhost:8000/reconcile`
- The backend responds with JSON containing a small preview

## Optional: point frontend to a different API

Set in `frontend/.env.local`:

```bash
NEXT_PUBLIC_API_BASE=https://your-api.example.com
```

