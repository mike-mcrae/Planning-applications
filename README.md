# Planning Applications Explorer (Dublin City)

Full-stack local web app for spatial exploration of Dublin City planning applications.

## Structure

- `backend/`: FastAPI API and spatial aggregation logic
- `frontend/`: React + Leaflet client
- `0. data/`: source data files used by backend
- `data/`: reserved for future app-specific derived exports

## Backend

From repo root:

```bash
source .venv/bin/activate
pip install -r backend/requirements.txt
uvicorn backend.app.main:app --reload --host 0.0.0.0 --port 8000
```

API endpoints:

- `GET /meta`
- `GET /applications`
- `GET /small_areas`
- `GET /electoral_divisions`
- `GET /summary`

Filters are query-parameter based and server-side.

### One-command run

From repo root:

```bash
./run_all.sh
```

This script:
- starts backend + frontend together
- auto-picks a free backend port if `8000` is already in use
- wires frontend API base URL to the chosen backend port

## Frontend

From `frontend/`:

```bash
npm install
npm run dev
```

The frontend defaults to `http://localhost:8000`. Override with:

```bash
VITE_API_BASE=http://localhost:8000 npm run dev
```

## Core Features Implemented

- Small Area choropleth layer
- Electoral Division choropleth layer (dissolved from SA)
- Raw planning application points
- Objection heat-style overlay
- Layer toggles
- Sidebar filters:
  - Time (year range + custom date range)
  - Time slider with play/pause animation
  - Development categories
  - Scale thresholds
  - Engagement thresholds
  - Outcome filters
- Dynamic summary panel:
  - Total applications
  - % with objection
  - Median letters
  - Letters per 1000 residents
  - Refusal rate
  - Appeal rate
- 95th percentile choropleth capping with explicit zero category metadata
- Shared metric scale computation for SA and ED per filter state
- Bounding box filtering for map movement
- Cached aggregate responses in-memory (`lru_cache`)

## Notes

- Population joins use:
  - SA JSON (`SAP2022T1T1ASA...json`) for SA-level population
  - ED CSV (`CensusHub2022_T9_1_ED...csv`) for ED-level population
- Spatial boundary source is `SMALL_AREA_2022.shp` filtered to `COUNTY_ENG == "DUBLIN CITY"`.
