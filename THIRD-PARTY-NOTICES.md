# Third-party notices

This project vendors and uses the following third-party components:

## Leaflet 1.9.4 (BSD-2-Clause)

- Vendored files: `frontend/leaflet.js`, `frontend/leaflet.css`,
  `frontend/images/*.png` (marker and layer icons).
- Source: https://leafletjs.com / https://unpkg.com/leaflet@1.9.4
- License: BSD 2-Clause “Simplified” License — © 2010–2024 Volodymyr Agafonkin,
  © 2010–2011, CloudMade. Full text:
  https://github.com/Leaflet/Leaflet/blob/main/LICENSE

## OpenStreetMap basemap tiles (ODbL)

- Runtime dependency only (no files vendored): raster tiles loaded from
  `tile.openstreetmap.org`; the app does not redistribute tiles.
- Data © OpenStreetMap contributors — Open Database License (ODbL):
  https://www.openstreetmap.org/copyright

## Python dependencies (requirements.txt)

- fastapi (MIT) — https://github.com/fastapi/fastapi
- uvicorn (BSD-3-Clause) — https://github.com/encode/uvicorn
- databricks-sql-connector (Apache-2.0) — https://github.com/databricks/databricks-sql-python
- databricks-sdk (Apache-2.0) — https://github.com/databricks/databricks-sdk-python
- pydantic (MIT) — https://github.com/pydantic/pydantic

Dependency licenses are satisfied by their distributions; this project adds
no modifications to them.