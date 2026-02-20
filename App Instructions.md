You are to build a full-stack interactive Planning Applications Explorer for Dublin City.

OBJECTIVE
Create a web-based interactive map application that allows users to explore planning applications spatially and filter them dynamically.

DATA SOURCES
1. applications_master_with_obs.csv
2. Small Area shapefile (SA_GUID_21)
3. Electoral Division shapefile (via dissolve or separate ED file)
4. 2022 Small Area population JSON
5. 2022 Electoral Division population CSV

CORE FEATURES

MAP LAYERS
- Small Area choropleth
- Electoral Division choropleth
- Raw application points
- Heatmap of objection letters
- Toggleable layers

APPLICATION POINT TOOLTIP
When clicking an application, display:
- Application number
- Address
- Development description
- Received date
- Decision date
- Number of units
- Site area
- Floor area
- Number of objection letters
- Decision outcome
- Appeal status
- Link to planning portal

FILTERS (Left Sidebar)

TIME FILTER
- Year slider
- Custom date range

DEVELOPMENT FILTERS
- Residential only
- Multi-unit only
- One-off houses only
- Commercial
- Extensions

SCALE FILTERS
- Min site area
- Min number of units
- High density (e.g. >10 units)

ENGAGEMENT FILTERS
- Has objection
- ≥ 5 letters
- ≥ 10 letters
- Top decile by letters

OUTCOME FILTERS
- Granted
- Refused
- Appealed
- Overturned

SUMMARY PANEL (Dynamic)
Display:
- Total applications
- % with objection
- Median letters
- Letters per 1000 residents
- Refusal rate
- Appeal rate

SCALING RULES
- Choropleths should cap at 95th percentile
- Zero values displayed as separate category
- Shared scale across SA and ED levels

ARCHITECTURE

Use:
- FastAPI backend
- Serve GeoJSON endpoints:
    /applications
    /small_areas
    /electoral_divisions
- Filtering performed server-side via query parameters

Frontend:
- React + Leaflet
- Dynamic layer toggles
- Real-time filtering via API calls
- Clean minimalist UI

DATA OPTIMISATION
- Precompute SA and ED aggregates
- Use spatial index
- Use bounding box filtering on map move
- Cache responses

OUTPUT
- Fully functional local web application
- Clear folder structure:
    /backend
    /frontend
    /data
- Include README with run instructions
- Include environment requirements file

OPTIONAL ADVANCED FEATURES
- Time animation slider
- Moran’s I computation
- Spatial clustering (LISA)
- Download filtered dataset as CSV
- Toggle raw letters list

Work autonomously.
Do not ask clarification questions unless absolutely necessary.
Build modular, clean, scalable code.
Document as you go.
