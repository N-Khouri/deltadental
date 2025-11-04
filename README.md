# Data Quality Dashboard

A lightweight Flask app for uploading CSVs, running basic data-quality checks, and viewing results in a friendly UI.
Uploads are stored on disk; summary metrics are persisted to SQLite for a browsable history. The app includes simple
request/latency logging for quick, zero-infra monitoring.
---
# Features
* CSV Upload & Validation — Upload ```.csv``` (≤10 MB). Server computes:
  * Missing values per column
  * Format checks (e.g., invalid emails, future dates, unrealistic ages, invalid statuses)
  * Logical inconsistencies (e.g., selling price < cost; stock below reorder level)
  * Duplicates (by email)
  * Outliers (invalid payment methods, negative/unknown totals, pricing anomalies)
* History View — Recent uploads with quick “issues at a glance” and expandable details.
* Modern, consistent UI — Dark theme, sticky header, responsive tables, and accessible controls for both pages.
* Zero-infra Monitoring — Rotating file logs for both app events and per-request access lines (with latency).
---
# Project Structure
```
.
├───backend
│   ├───logs
│   ├───uploads
│   └───app.py
├───frontend
│   └───templates
│       ├───404.html
│       ├───history.html
│       └───index.html
└───requirements.txt
```
* Flask is configured to load templates from frontend/templates and serves:
  * ```/``` &rarr; ```index.html```
  * ```/history``` &rarr; ```history.html``` (recent uploads)
  * 404 &rarr; ```404.html``` (simple page; place alongside your other templates as needed)
---
# Prerequisites 
* Python 3.10+ (tested with 3.11/3.12)
* pip / venv
* (Linux/macOS/WSL/Windows supported)
---
# Quick Start (Local Development)
1. Clone & enter the project
```
git clone https://github.com/N-Khouri/deltadental
cd deltadental
```
2. Create a virtual environment & install deps
```
python -m venv .venv
pip install -r requirements.txt
```
3. Run the app
```
cd backend
python app.py
```
4. Open the UI
* Upload page: http:localhost:5000
* History page: http:localhost:5000/history

The app will create on first use:
* ```backend/uploads/``` (uploaded files)
* ```backend/logs``` (rotating logs: app.log and access.log)
* app.db (SQLite) at the project root (relative to the working directory)
---
# How It Works (High-Level)
* Upload flow (```POST /upload```)
  1. Validate file type/size (```.csv```, <= 10 MB)
  2. Saves to ```uploads/```, reads with pandas, computes metrics described above.
  3. Persists a row per upload into SQLite (```uploads``` table), including JSON blobs for each metric group.
  4. Returns a JSON payload the UI renders immediately.
* History flow (```GET /history```)
  1. Reads the latest 100 uploads from SQLite, converts stored JSON into “top” lists, and renders an expandable table.
* UI
  1. ```index.html``` handles file selection, shows per-section tables for results, and resets the picker on success.
  2. ```history.html``` shows a compact, filterable table with expandable details per upload.
---
# User Guide
1. Upload a CSV
   * Go to `/` &rarr; "Choose file" &rarr; select a `.csv` (≤10 MB) &rarr; click **Upload CSV**.
   * The page shows summary (filename, path, row/column counts), plus tables for missing values, formats, logics, duplicates, and outliers (only visible if non-zero).
2. Review History
   * Go to `/history`. Use the search box to filter by filename or ID; click View to expand issue details per upload. 
---
# Configuration
Key constants (edit in `backend/app.py` if desired)
* `ALLOWED_EXTENSIONS = {"csv"}`
* `MAX_CONTENT_LENGTH = 10 * 1024 * 1024` (10 MB upload cap)
* `DB_PATH = "app.db"` (SQLite path)
* `app.config["UPLOAD_FOLDER"] = "uploads"` (relative to working dir)
* Template folder: ```template_folder='../frontend/templates'```
> Ensure the process user has write access to uploads/, logs/, and the directory holding app.db.
---
# Logging & Monitoring (Built-In)
The app initializes rotating logs on startup:
* `logs/access.log` - one line per request: method, path, status, bytes, dur_ms, IP, user-agent.
* `logs/app.log` - app messages and full stack traces on errors.
* Exceptions return JSON `{"error": "Internal Server Error"}` with a 500 status.

## View logs live
```
# macOS/Linux
tail -f backend/logs/access.log backend/logs/app.log
# Windows PowerShell
Get-Content .\backend\logs\access.log -Wait
Get-Content .\backend\logs\app.log -Wait
```
## Sanity Tests
```
# macOS/Linux (or if you have git bash installed on Windows)
# 400 (no file)
curl -i -X POST http://localhost:5000/upload

# Successful upload
printf "email,total_amount\nalice@example.com,10\n" > sample.csv
curl -i -X POST -F "file=@sample.csv" http://localhost:5000/upload
```
> You should see corresponding entries in both logs (200s/400s and durations).
---
# Database
* On first run, the app creates `uploads` table if it does not exist. Each upload stores:
* `filename`, `saved_to`, `row_count`, `column_count`, `uploaded_at`
* JSON fields: `nulls_json`, `formats_json`, `logical_inconsistencies_json`, `duplicate_records_json`, `outliers_json`, `summary_json` 
---
# Development Notes & Design Decisions
* **Template location**: Kept under `frontend/templates` for a tidy separation of UI and backend. Flask is pointed there explicitly.
* **Dark, accessible UI**: Consistent CSS variables, sticky headers, responsive tables/cards; upload and history screens share the same design language for familiarity.
* **Data checks**: Focused on pragmatic, explainable rules (missing/nulls, formats, simple logics, duplicates, obvious outliers) so results are easy to interpret from sample business datasets. 
* **SQLite first**: Zero-config persistence to simplify evaluation and local dev; schema created automatically. 
* **Monitoring**: Minimal rotating logs give immediate visibility (status/latency/errors) without external services.