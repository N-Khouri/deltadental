from flask import Flask, request, render_template, jsonify
from werkzeug.utils import secure_filename
import os
import pandas as pd
import sqlite3
from datetime import datetime
import json

ALLOWED_EXTENSIONS = {"csv"}
MAX_CONTENT_LENGTH = 10 * 1024 * 1024  # 10 MB
DB_PATH = "app.db"

app = Flask(__name__, template_folder='../frontend/templates')
app.config["UPLOAD_FOLDER"] = "uploads"
app.config["MAX_CONTENT_LENGTH"] = MAX_CONTENT_LENGTH

os.makedirs(app.config["UPLOAD_FOLDER"], exist_ok=True)

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with get_db() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS uploads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filename TEXT NOT NULL,
                saved_to TEXT NOT NULL,
                row_count INTEGER,
                column_count INTEGER,
                nulls_json TEXT,            -- JSON array of {column, missing, missing_pct}
                uploaded_at TEXT NOT NULL   -- ISO timestamp
            );
            """
        )


init_db()


def allowed_file(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


@app.get("/")
def index():
    return render_template("index.html")

@app.get("/history")
def history_page():
    with get_db() as conn:
        rows = conn.execute(
            "SELECT id, filename, saved_to, row_count, column_count, nulls_json, uploaded_at FROM uploads ORDER BY id DESC LIMIT 100"
        ).fetchall()
    records = []
    for r in rows:
        try:
            nulls = json.loads(r["nulls_json"]) if r["nulls_json"] else []
        except Exception:
            nulls = []
        top3 = [f"{n.get('column')}: {n.get('missing_pct', 0)}%" for n in nulls[:3]]
        records.append({
            "id": r["id"],
            "filename": r["filename"],
            "saved_to": r["saved_to"],
            "row_count": r["row_count"],
            "column_count": r["column_count"],
            "uploaded_at": r["uploaded_at"],
            "top3": top3
        })
    return render_template("history.html", records=records)


@app.post("/upload")
def upload():
    if "file" not in request.files:
        return jsonify({"error": "No file part in request."}), 400

    file = request.files["file"]

    if file.filename == "":
        return jsonify({"error": "No file selected."}), 400

    if not allowed_file(file.filename):
        return jsonify({"error": "Only .csv files are allowed."}), 400

    filename = secure_filename(file.filename)
    dest_path = os.path.join(app.config["UPLOAD_FOLDER"], filename)
    file.save(dest_path)

    try:
        df = pd.read_csv(dest_path, low_memory=False)
        rows, cols = df.shape
        columns = df.columns.tolist()

        null_counts = df.isna().sum()
        null_pct = (df.isna().mean() * 100).round(2)
        nulls = (
            pd.DataFrame({"column": df.columns, "missing": null_counts.values, "missing_pct": null_pct.values})
            .sort_values(["missing", "column"], ascending=[False, True])
            .to_dict(orient="records")
        )

    except Exception as e:
        with get_db() as conn:
            conn.execute(
                "INSERT INTO uploads (filename, saved_to, row_count, column_count, nulls_json, uploaded_at) VALUES (?, ?, ?, ?, ?, ?)",
                (filename, dest_path, None, None, None, datetime.utcnow().isoformat()),
            )
        return jsonify({
            "message": "Upload successful, but failed to read CSV for metadata.",
            "filename": filename,
            "saved_to": dest_path,
            "read_error": str(e)
        }), 200

    with get_db() as conn:
        conn.execute(
            "INSERT INTO uploads (filename, saved_to, row_count, column_count, nulls_json, uploaded_at) VALUES (?, ?, ?, ?, ?, ?)",
            (filename, dest_path, int(rows), int(cols), json.dumps(nulls), datetime.utcnow().isoformat()),
        )

    return jsonify({
        "message": "Upload successful",
        "filename": filename,
        "saved_to": dest_path,
        "row_count": rows,
        "column_count": cols,
        "columns": columns[:50],
        "nulls": nulls
    }), 200


if __name__ == "__main__":
    app.run(debug=True)