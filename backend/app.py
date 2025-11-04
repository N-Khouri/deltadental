import numpy as np
from flask import Flask, request, render_template, jsonify, g
from werkzeug.utils import secure_filename
import os
import pandas as pd
import sqlite3
from datetime import datetime, timezone
import json
import re
from collections import Counter
import logging, time
from logging.handlers import RotatingFileHandler


ALLOWED_EXTENSIONS = {"csv"}
MAX_CONTENT_LENGTH = 10 * 1024 * 1024  # 10 MB
DB_PATH = "app.db"
log_dir = "logs"

app = Flask(__name__, template_folder='../frontend/templates')
app.config["UPLOAD_FOLDER"] = "uploads"
app.config["MAX_CONTENT_LENGTH"] = MAX_CONTENT_LENGTH

os.makedirs(app.config["UPLOAD_FOLDER"], exist_ok=True)
os.makedirs(log_dir, exist_ok=True)

app_handler = RotatingFileHandler(os.path.join(log_dir, "app.log"), maxBytes=1_000_000, backupCount=5)
app_handler.setLevel(logging.INFO)
app_handler.setFormatter(logging.Formatter(
    "%(asctime)s %(levelname)s %(name)s :: %(message)s"
))
app.logger.addHandler(app_handler)
app.logger.setLevel(logging.INFO)

access_logger = logging.getLogger("access")
access_handler = RotatingFileHandler(os.path.join(log_dir, "access.log"), maxBytes=5_000_000, backupCount=3)
access_handler.setLevel(logging.INFO)
access_handler.setFormatter(logging.Formatter("%(message)s"))
access_logger.addHandler(access_handler)
access_logger.setLevel(logging.INFO)

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
                logical_inconsistencies_json TEXT,
                duplicate_records_json TEXT,
                outliers_json TEXT,
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
            "nulls_json, formats_json, logical_inconsistencies_json, duplicate_records_json, outliers_json, summary_json, "
            "uploaded_at FROM uploads ORDER BY id DESC LIMIT 100"
        ).fetchall()
    records = []
    for r in rows:
        try:
            nulls = json.loads(r["nulls_json"]) if r["nulls_json"] else []
            formats = json.loads(r["formats_json"]) if r["formats_json"] else {}
            logics = json.loads(r["logical_inconsistencies_json"]) if r["logical_inconsistencies_json"] else {}
            duplicates = json.loads(r["duplicate_records_json"]) if r["duplicate_records_json"] else {}
            outliers = json.loads(r["outliers_json"]) if r["outliers_json"] else {}
        except Exception:
            nulls = []
            formats = []
            logics = []
            duplicates = []
            outliers = []
        top_nulls = [f"{n.get('column')}: {n.get('missing_pct', 0)}%" for n in (nulls or []) if n.get('missing_pct', 0) > 0]
        top_formats = [f"{key}: {value}{'%' if 'pct' in key else ''}" for key, value in (formats.items() or {}) if value > 0]
        top_logics = [f"{key}: {value}{'%' if 'pct' in key else ''}" for key, value in (logics.items() or {}) if value > 0]
        top_duplicates = [f"{key}: {value}{'%' if 'pct' in key else ''}" for key, value in (duplicates.items() or {}) if value > 0]
        top_outliers = [f"{key}: {value}{'%' if 'pct' in key else ''}" for key, value in (outliers.items() or {}) if value > 0]

        records.append({
            "id": r["id"],
            "filename": r["filename"],
            "saved_to": r["saved_to"],
            "row_count": r["row_count"],
            "column_count": r["column_count"],
            "uploaded_at": r["uploaded_at"],
            "top_nulls": top_nulls,
            "top_formats": top_formats,
            "top_logics": top_logics,
            "top_duplicates": top_duplicates,
            "top_outliers": top_outliers,
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
    dups, outliers, rules, summary = {}, {}, {}, {}

    try:
        # Read the files
        df = pd.read_csv(dest_path, low_memory=False)
        rows, cols = df.shape
        columns = df.columns.tolist()

        # Calculate missing entries for all columns
        null_counts = df.isna().sum()
        null_pct = (df.isna().mean() * 100).round(2)
        nulls = (
            pd.DataFrame({"column": df.columns,
                          "missing": null_counts.values,
                          "missing_pct": null_pct.values})
            .sort_values(["missing", "column"], ascending=[False, True])
            .to_dict(orient="records")
        )
        nulls_light = nulls

        # Calculate any format errors for all columns
        invalid_emails = 0
        # validate if an email follows the norm. (e.g., @ exists, domain exists...)
        email_re = re.compile(r"^[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*@(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$")
        if "email" in df.columns:
            ser = df["email"]
            ser = ser.dropna()
            mask = ~ser.str.fullmatch(email_re)
            invalid_emails += int(mask.sum())
        invalid_emails_pct = round((invalid_emails / rows) * 100, 3)

        today = datetime.now(timezone.utc).isoformat()
        header = [c for c in df.columns if "date" in c.lower()]
        dates = [pd.to_datetime(c, format='mixed').isoformat() for c in df[header[0]]]
        total_future_dates = 0
        for date_col in dates:
            if date_col > today:
                total_future_dates += 1
        total_future_dates_pct = round((total_future_dates / rows) * 100 if rows else 0.0, 3)

        total_unrealistic_ages = 0
        if "age" in df.columns:
            for age in df["age"]:
                if age < 18 or age > 100:
                    total_unrealistic_ages += 1
        total_unrealistic_pct = round((total_unrealistic_ages / rows) * 100 if rows else 0.0, 3)

        total_invalid_statuses = 0
        if "status" in df.columns:
            status_col = df["status"].dropna().astype(str)
            total_invalid_statuses += int((status_col == "UNKNOWN").sum())
        total_invalid_statuses_pct = round((total_invalid_statuses / rows) * 100 if rows else 0.0, 3)

        formats = {
            "email_invalid": invalid_emails,
            "email_invalid_pct": invalid_emails_pct,
            "future_dates": total_future_dates,
            "future_dates_pct": total_future_dates_pct,
            "unrealistic_ages": total_unrealistic_ages,
            "total_unrealistic_pct": total_unrealistic_pct,
            "invalid_statuses": total_invalid_statuses,
            "invalid_statuses_pct": total_invalid_statuses_pct,
        }

        # Calculate any logical inconsistencies found for all columns
        sell_less_cost = 0
        sell_less_cost_pct = 0
        if "cost_price" in df.columns and "selling_price" in df.columns:
            cost = pd.to_numeric(df["cost_price"], errors="coerce")
            sell = pd.to_numeric(df["selling_price"], errors="coerce")

            valid = cost.notna() & sell.notna()
            violate = (sell < cost) & valid

            total_violations = int(violate.sum())

            sell_less_cost += total_violations
            sell_less_cost_pct += round((total_violations / rows) * 100 if rows else 0.0, 3)

        stock_less_reorder_pct = 0
        stock_less_reorder = 0
        if "current_stock" in df.columns and "reorder_level" in df.columns:
            current_stock = pd.to_numeric(df["current_stock"], errors="coerce")
            reorder_level = pd.to_numeric(df["reorder_level"], errors="coerce")
            valid = current_stock.notna() & reorder_level.notna()
            violate = (current_stock < reorder_level) & valid

            total_violations = int(violate.sum())

            stock_less_reorder += total_violations
            stock_less_reorder_pct += round((total_violations / rows) * 100 if rows else 0.0, 3)

        logical_inconsistencies = {
            "sell_less_cost": sell_less_cost,
            "sell_less_cost_pct": sell_less_cost_pct,
            "stock_less_reorder": stock_less_reorder,
            "stock_less_reorder_pct": stock_less_reorder_pct,
        }

        # Calculate how many duplicate records where found. Determined by repeated emails.
        total_duplicates_records = 0
        total_duplicates_records_pct = 0
        if "email" in df.columns:
            dups_by_email = int(df["email"].dropna().duplicated().sum())
            total_duplicates_records += dups_by_email
            total_duplicates_records_pct += round((dups_by_email / rows) * 100 if rows else 0.0, 3)
        print(total_duplicates_records_pct)

        duplicate_records = {
            "total_duplicates_records": total_duplicates_records,
            "total_duplicates_records_pct": total_duplicates_records_pct,
        }

        # Calculate any outliers found. E.g., invalid payment methods, negative total amounts/errors, ...
        total_invalid_methods = 0
        total_invalid_methods_pct = 0
        if "payment_method" in df.columns:
            im = df["payment_method"].dropna().astype(str)
            total_invalid_methods += int((im == "INVALID_METHOD").sum())

            total_invalid_methods_pct += round((total_invalid_methods / rows) * 100 if rows else 0.0, 3)

        negative_total_amount = 0
        negative_total_amount_pct = 0
        error_total_amount = 0
        error_total_amount_pct = 0
        if "total_amount" in df.columns:
            total_amount = df["total_amount"].dropna()
            tm = pd.to_numeric(total_amount, errors="coerce")

            negative_total_amount += int((tm < 0).sum())
            error_total_amount += int((tm.isna()).sum())

            negative_total_amount_pct += round((negative_total_amount / rows) * 100 if rows else 0.0, 3)
            error_total_amount_pct += round((error_total_amount / rows) * 100 if rows else 0.0, 3)

        total_pricing_error = 0
        total_pricing_error_pct = 0
        if "product_id" in df.columns and "unit_price" in df.columns and "product_name" in df.columns:
            unit_price = pd.to_numeric(df["unit_price"], errors="coerce").round(2)
            tmp = df.assign(__price_cents=unit_price)

            def canonical_price(series):
                s = series.dropna()
                if s.empty:
                    return np.nan
                counts = Counter(s)
                max_count = max(counts.values())
                modes = [p for p, c in counts.items() if c == max_count]
                return float(np.nanmin(modes))

            grp = tmp.groupby(["product_id", "product_name"], dropna=False)
            canon = grp["__price_cents"].apply(canonical_price).rename("canonical_unit_price")
            res = tmp.merge(canon, on=["product_id", "product_name"], how="left")

            valid = res["__price_cents"].notna() & res["canonical_unit_price"].notna()
            res["is_outlier"] = valid & (res["__price_cents"] != res["canonical_unit_price"])
            total_pricing_error += int((res["is_outlier"] == True).sum())
            total_pricing_error_pct += round((total_pricing_error / rows) * 100 if rows else 0.0, 3)

        outliers = {
            "total_invalid_methods": total_invalid_methods,
            "total_invalid_methods_pct": total_invalid_methods_pct,
            "negative_total_amount": negative_total_amount,
            "negative_total_amount_pct": negative_total_amount_pct,
            "error_total_amount": error_total_amount,
            "error_total_amount_pct": error_total_amount_pct,
            "total_pricing_error": total_pricing_error,
            "total_pricing_error_pct": total_pricing_error_pct,
        }

        summary = {
            "missing_cols": int((null_counts > 0).sum()),
            "formats_total": int(formats.get("email_invalid", 0))
                             + int(formats.get("future_dates", 0))
                             + int(formats.get("unrealistic_ages", 0)),
            "logical_inconsistencies": logical_inconsistencies,
            "duplicates": sum(v for v in dups.values() if isinstance(v, int)),
            "outliers": sum(outliers.values()) if outliers else 0,
            "rule_violations": sum(rules.get("violations", {}).values()) if isinstance(rules, dict) else 0,
        }

    except Exception as e:
        with get_db() as conn:
            try:
                conn.execute(
                    "INSERT INTO uploads (filename, saved_to, row_count, column_count, "
                    "nulls_json, formats_json, logical_inconsistencies_json, duplicate_records_json, outliers_json, summary_json, uploaded_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (filename, dest_path, None, None, None, None, None, None, None, None, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                )
            except Exception:
                conn.execute(
                    "INSERT INTO uploads (filename, saved_to, row_count, column_count, nulls_json, uploaded_at) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (filename, dest_path, None, None, None, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
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
            "nulls_json, formats_json, logical_inconsistencies_json, duplicate_records_json, outliers_json, summary_json, uploaded_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                filename, dest_path, int(rows), int(cols),
                json.dumps(nulls_light),
                json.dumps(formats), json.dumps(logical_inconsistencies), json.dumps(duplicate_records),
                json.dumps(outliers),
                json.dumps(summary), datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            ),
        )

    return jsonify({
        "message": "Upload successful",
        "filename": filename,
        "saved_to": dest_path,
        "row_count": rows or 0,
        "column_count": cols or 0,
        "columns": columns if columns else [],
        "nulls": nulls_light,
        "formats": formats,
        "logical_inconsistencies": logical_inconsistencies,
        "duplicates": duplicate_records,
        "outliers": outliers,
        "rules": rules,
        "summary": summary
    }), 200

@app.before_request
def _start_timer():
    g._t0 = time.perf_counter()

@app.after_request
def _log_request(resp):
    try:
        dt_ms = int((time.perf_counter() - getattr(g, "_t0", time.perf_counter())) * 1000)
        line = (
            f'method={request.method} path="{request.path}" '
            f'status={resp.status_code} bytes={resp.calculate_content_length() or 0} '
            f'dur_ms={dt_ms} ip={request.headers.get("X-Forwarded-For", request.remote_addr)} '
            f'ua="{request.user_agent.string}"'
        )
        logging.getLogger("access").info(line)
    except Exception as e:
        app.logger.exception("failed to write access log: %s", e)
    return resp

@app.errorhandler(Exception)
def _on_error(e):
    app.logger.exception("unhandled exception")
    return jsonify({"error": "Internal Server Error"}), 500

@app.errorhandler(404)
def page_not_found(e):
    return render_template('404.html'), 404

if __name__ == "__main__":
    app.run(debug=True)