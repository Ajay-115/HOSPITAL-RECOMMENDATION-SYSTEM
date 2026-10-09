"""
Real-Time Hospital Resource, Surgery and Blood Donation Recommendation System
Backend Entrypoint & REST API Server (Flask + CORS)
"""


import os
import sys

# Ensure the project root is on sys.path so that 'spark.*' packages are
# importable regardless of the working directory (including Flask's reloader).
_BACKEND_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.abspath(os.path.join(_BACKEND_DIR, ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from flask import Flask, jsonify, send_from_directory
from flask_cors import CORS


from routes.hospital_routes import hospital_bp
from routes.surgery_routes import surgery_bp
from routes.blood_routes import blood_bp
from routes.admin_routes import admin_bp
from routes.realtime_routes import realtime_bp

# Define frontend path for static delivery
BACKEND_DIR = os.path.dirname(os.path.abspath(__file__))
FRONTEND_DIR = os.path.abspath(os.path.join(BACKEND_DIR, "..", "frontend"))

app = Flask(__name__, static_folder=FRONTEND_DIR, static_url_path="")
CORS(app)  # Enable Cross-Origin Resource Sharing for API requests

from database.database import init_db, DEFAULT_DB_PATH

# Configuration support for database path (production default or test override)
app.config["DATABASE_PATH"] = os.environ.get("DATABASE_PATH", DEFAULT_DB_PATH)

# Initialize database schema and seeds if needed using configured path
init_db(app.config["DATABASE_PATH"])

# Register Blueprints
app.register_blueprint(hospital_bp)
app.register_blueprint(surgery_bp)
app.register_blueprint(blood_bp)
app.register_blueprint(admin_bp)
app.register_blueprint(realtime_bp)


# Health Check
@app.route("/api/health", methods=["GET"])
def health_check():
    return jsonify({
        "status": "healthy",
        "project": "Real-Time Hospital Resource, Surgery and Blood Donation Recommendation System",
        "phase": "Phase 5 (SQLite Persistent Database + Admin CRUD Integration)",
        "academic_notice": "Information shown for academic demonstration purposes only."
    }), 200


# Frontend Static File Fallbacks
@app.route("/", methods=["GET"])
def serve_home():
    return send_from_directory(FRONTEND_DIR, "index.html")


@app.route("/<path:path>", methods=["GET"])
def serve_static(path):
    target = os.path.join(FRONTEND_DIR, path)
    if os.path.exists(target):
        return send_from_directory(FRONTEND_DIR, path)
    return send_from_directory(FRONTEND_DIR, "index.html")


# Error Handlers
@app.errorhandler(404)
def not_found_error(error):
    return jsonify({
        "success": False,
        "error": "The requested resource or endpoint was not found.",
        "code": 404
    }), 404


@app.errorhandler(500)
def internal_error(error):
    return jsonify({
        "success": False,
        "error": "An internal server error occurred while processing the request.",
        "code": 500
    }), 500


if __name__ == "__main__":
    print("\n=======================================================")
    print(" MediPulse: Healthcare Resource Recommendation System ")
    print(" Final-Year B.Tech Capstone Project API Server         ")
    print("=======================================================")
    print(f" * Serving Frontend from: {FRONTEND_DIR}")
    print(" * API Server running at: http://localhost:5000")
    print(" * Press CTRL+C to terminate\n")
    app.run(host="0.0.0.0", port=5000, debug=True)
