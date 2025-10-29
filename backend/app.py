from flask import Flask, request, render_template, jsonify
from werkzeug.utils import secure_filename
import os
import pandas as pd

ALLOWED_EXTENSIONS = {"csv"}
MAX_CONTENT_LENGTH = 10 * 1024 * 1024  # 10 MB

app = Flask(__name__, template_folder='../frontend/templates')
app.config["UPLOAD_FOLDER"] = "uploads"
app.config["MAX_CONTENT_LENGTH"] = MAX_CONTENT_LENGTH

os.makedirs(app.config["UPLOAD_FOLDER"], exist_ok=True)


def allowed_file(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


@app.get("/")
def index():
    return render_template("index.html")


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
        # Let pandas auto-detect dialect; low_memory=False helps wide CSVs
        df = pd.read_csv(dest_path, low_memory=False)
        rows, cols = df.shape
        columns = df.columns.tolist()
    except Exception as e:
        return jsonify({
            "message": "Upload successful, but failed to read CSV for metadata.",
            "filename": filename,
            "saved_to": dest_path,
            "read_error": str(e)
        }), 200

    return jsonify({
        "message": "Upload successful",
        "filename": filename,
        "saved_to": dest_path,
        "row_count": rows,
        "column_count": cols,
        "columns": columns[:50]
    }), 200


if __name__ == "__main__":
    app.run(debug=True)