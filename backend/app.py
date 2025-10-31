from flask import Flask, request, render_template, jsonify
from werkzeug.utils import secure_filename
import os
import pandas as pd
import sqlite3
from datetime import datetime, timezone
import json
import re

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
                formats_json TEXT,
                duplicates_json TEXT,       -- simple duplicate counts
                outliers_json TEXT,         -- numeric outlier counts per column
                rules_json TEXT,            -- business-rule violations
                summary_json TEXT,          -- small rollup counts
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
            "SELECT id, filename, saved_to, row_count, column_count, "
            "nulls_json, emails, future_date, ages, duplicates_json, outliers_json, rules_json, summary_json, "
            "uploaded_at FROM uploads ORDER BY id DESC LIMIT 100"
        ).fetchall()
    records = []
    for r in rows:
        try:
            nulls = json.loads(r["nulls_json"]) if r["nulls_json"] else []
        except Exception:
            nulls = []
        top3_null = [f"{n.get('column')}: {n.get('missing_pct', 0)}%" for n in (nulls or [])[:3]]

        try:
            formats = json.loads(r["formats_json"]) if r["formats_json"] else {}
            dups = json.loads(r["duplicates_json"]) if r["duplicates_json"] else {}
            outliers = json.loads(r["outliers_json"]) if r["outliers_json"] else {}
            rules = json.loads(r["rules_json"]) if r["rules_json"] else {}
        except Exception:
            formats, dups, outliers, rules = {}, {}, {}, {}

        counts = {
            "formats": int(formats.get("email_invalid", 0))
                       + (sum(formats.get("future_dates", {}).values())
                          if isinstance(formats.get("future_dates"), dict) else 0),
            "duplicates": sum(v for v in dups.values() if isinstance(v, int)),
            "outliers": sum(outliers.values()) if isinstance(outliers, dict) else 0,
            "rules": sum(rules.get("violations", {}).values()) if isinstance(rules, dict) else 0,
        }

        records.append({
            "id": r["id"],
            "filename": r["filename"],
            "saved_to": r["saved_to"],
            "row_count": r["row_count"],
            "column_count": r["column_count"],
            "uploaded_at": r["uploaded_at"],
            "top3_null": top3_null,
            "counts": counts,
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

    rows = cols = 0
    columns = []
    nulls_light, types_light = [], []
    formats = {}
    dups, outliers, rules, summary = {}, {}, {}, {}

    try:
        df = pd.read_csv(dest_path, low_memory=False)
        rows, cols = df.shape
        columns = df.columns.tolist()

        null_counts = df.isna().sum()
        null_pct = (df.isna().mean() * 100).round(2)
        nulls = (
            pd.DataFrame({"column": df.columns,
                          "missing": null_counts.values,
                          "missing_pct": null_pct.values})
            .sort_values(["missing", "column"], ascending=[False, True])
            .to_dict(orient="records")
        )
        nulls_light = nulls[:500]

        invalid_emails = 0
        email_re = re.compile(r"^[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*@(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$")
        if "email" in df.columns:
            ser = df["email"]
            ser = ser.dropna()
            mask = ~ser.str.fullmatch(email_re)
            invalid_emails += int(mask.sum())

        today = datetime.now(timezone.utc).isoformat()
        header = [c for c in df.columns if "date" in c.lower()]
        dates = [pd.to_datetime(c, format='mixed').isoformat() for c in df[header[0]]]
        total_future_dates = 0
        for date_col in dates:
            if date_col > today:
                total_future_dates += 1

        total_unrealistic_ages = 0
        if "age" in df.columns:
            for age in df["age"]:
                if age < 18 or age > 100:
                    total_unrealistic_ages += 1

        formats = {
            "email_invalid": invalid_emails,
            "future_dates": total_future_dates,
            "unrealistic_ages": total_unrealistic_ages,
        }

        # Probe 4: Duplicates
        if "email" in df.columns:
            dups_by_email = int(df["email"].dropna().duplicated().sum())
            dups["by_email"] = dups_by_email
        dups["full_row"] = int(df.duplicated().sum())

        # Probe 5: Outliers (IQR on numeric columns)
        outliers = {}
        num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
        for c in num_cols:
            s = df[c].dropna()
            if s.empty:
                continue
            q1, q3 = s.quantile(0.25), s.quantile(0.75)
            iqr = q3 - q1
            if iqr == 0:
                continue
            lower, upper = q1 - 1.5 * iqr, q3 + 1.5 * iqr
            outliers[c] = int(((s < lower) | (s > upper)).sum())

        # Probe 6: Business-rule checks (dataset-aware)
        rules = {"violations": {}}
        cols_lower = {c.lower(): c for c in df.columns}

        # Inventory: selling >= cost
        if all(k in cols_lower for k in ("cost_price","selling_price")):
            cp, sp = cols_lower["cost_price"], cols_lower["selling_price"]
            mask = (pd.to_numeric(df[sp], errors="coerce")
                    < pd.to_numeric(df[cp], errors="coerce"))
            rules["violations"]["selling_below_cost"] = int(mask.sum())

        # Inventory: stock >= reorder level
        if all(k in cols_lower for k in ("current_stock","reorder_level")):
            cs, rl = cols_lower["current_stock"], cols_lower["reorder_level"]
            mask = (pd.to_numeric(df[cs], errors="coerce")
                    < pd.to_numeric(df[rl], errors="coerce"))
            rules["violations"]["stock_below_reorder"] = int(mask.sum())

        # Customers: realistic age
        if "age" in cols_lower:
            age = pd.to_numeric(df[cols_lower["age"]], errors="coerce")
            rules["violations"]["unrealistic_age"] = int(((age < 0) | (age > 120)).sum())

        # Transactions: non-negative totals
        if "total_amount" in cols_lower:
            ta = pd.to_numeric(df[cols_lower["total_amount"]], errors="coerce")
            rules["violations"]["negative_total_amount"] = int((ta < 0).sum())

        # Transactions: placeholder invalid method example
        if "payment_method" in cols_lower:
            pm = df[cols_lower["payment_method"]].astype(str)
            rules["violations"]["invalid_payment_method"] = int((pm == "INVALID_METHOD").sum())

        # Summary rollup
        summary = {
            "missing_cols": int((null_counts > 0).sum()),
            "formats_total": int(formats.get("email_invalid", 0))
                             + int(formats.get("future_dates", 0))
                             + int(formats.get("unrealistic_ages", 0)),
            "duplicates": sum(v for v in dups.values() if isinstance(v, int)),
            "outliers": sum(outliers.values()) if outliers else 0,
            "rule_violations": sum(rules.get("violations", {}).values()) if isinstance(rules, dict) else 0,
        }

    except Exception as e:
        with get_db() as conn:
            try:
                conn.execute(
                    "INSERT INTO uploads (filename, saved_to, row_count, column_count, "
                    "nulls_json, formats_json, duplicates_json, outliers_json, rules_json, summary_json, uploaded_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (filename, dest_path, None, None, None, None, None, None, None, None, datetime.now(timezone.utc).isoformat()),
                )
            except Exception:
                conn.execute(
                    "INSERT INTO uploads (filename, saved_to, row_count, column_count, nulls_json, uploaded_at) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (filename, dest_path, None, None, None, datetime.now(timezone.utc).isoformat()),
                )

        return jsonify({
            "message": "Upload successful, but failed to read CSV for metadata.",
            "filename": filename,
            "saved_to": dest_path,
            "read_error": str(e)
        }), 200

    with get_db() as conn:
        conn.execute(
            "INSERT INTO uploads (filename, saved_to, row_count, column_count, "
            "nulls_json, formats_json, duplicates_json, outliers_json, rules_json, summary_json, uploaded_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                filename, dest_path, int(rows), int(cols),
                json.dumps(nulls_light),
                json.dumps(formats),json.dumps(dups),
                json.dumps(outliers), json.dumps(rules),
                json.dumps(summary), datetime.now(timezone.utc).isoformat(),
            ),
        )

    return jsonify({
        "message": "Upload successful",
        "filename": filename,
        "saved_to": dest_path,
        "row_count": rows or 0,
        "column_count": cols or 0,
        "columns": columns[:50] if columns else [],
        "nulls": nulls_light,
        "formats": formats,
        "duplicates": dups,
        "outliers": outliers,
        "rules": rules,
        "summary": summary
    }), 200


if __name__ == "__main__":
    app.run(debug=True)