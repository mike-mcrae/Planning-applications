# Planning Applications Explorer (Dublin City)

Production-ready full-stack app for exploring Dublin City planning applications.

## Folder Structure

```text
/backend
  app.py
  main.py
  requirements.txt
/frontend
  package.json
  src/
/data
README.md
render.yaml
```

Notes:
- Backend supports both `/data` (production) and legacy local paths (`0. data`, `1. scripts`) automatically.

## Backend (FastAPI)

Run locally from `backend/`:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app:app --reload
```

Render production start command:

```bash
uvicorn app:app --host 0.0.0.0 --port $PORT
```

### Required API endpoints

- `GET /applications`
- `GET /small_areas`
- `GET /electoral_divisions`

Supported filter params include:
- `?year=`
- `?min_units=`
- `?has_objection=`
- `?decision=`

Also supported:
- `year_min`, `year_max`, `development`, `top_decile`, `outcomes`, bbox params, etc.

### Backend production features

- Startup precomputation of joins/aggregates
- Bounding-box filtering
- GZip compression middleware
- CORS middleware with `FRONTEND_ORIGIN` env var

## Frontend (React + Vite)

From `frontend/`:

```bash
npm install
npm run build
```

Build output is generated to:

- `frontend/build`

Frontend API URL is configured at build-time via:

- `REACT_APP_API_URL`

Example:

```bash
REACT_APP_API_URL=https://your-backend.onrender.com npm run build
```

## Render Deployment

`render.yaml` is included for two services:

1. `planning-explorer-backend` (Python web service)
2. `planning-explorer-frontend` (Static site)

### Backend env vars

- `FRONTEND_ORIGIN=https://your-frontend.onrender.com`

### Frontend env vars

- `REACT_APP_API_URL=https://your-backend.onrender.com`

### Deploy

1. Push repo to GitHub.
2. In Render, create Blueprint deploy from repo (uses `render.yaml`).
3. Verify backend starts with `uvicorn app:app --host 0.0.0.0 --port $PORT`.
4. Verify frontend static publish path is `build`.
5. Open frontend public URL and test map + filters.
