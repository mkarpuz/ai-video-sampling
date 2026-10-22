"""Run one YouTube search, or save a previously downloaded response."""

import argparse
import json
import logging
import os
from pathlib import Path
import sys
import tempfile
from uuid import UUID
from zipfile import ZipFile, ZIP_DEFLATED

import psycopg
import requests

import database


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
PENDING = DATA / "pending"

EXPORT_QUERIES = {
    "videos.csv": """
        SELECT video_id AS id, metadata->>'title' AS title,
               metadata->'channel'->>'name' AS channel,
               metadata->>'viewCount' AS "viewCount",
               metadata->>'uploadDate' AS "uploadDate",
               metadata->>'duration' AS duration, first_seen_at, last_seen_at
        FROM videos ORDER BY first_seen_at, video_id
    """,
    "search_history.csv": """
        SELECT run_id, started_at, finished_at, status,
               settings->>'query' AS query, settings->>'type' AS type,
               settings->>'uploadDate' AS "uploadDate",
               settings->>'sortBy' AS "sortBy", settings->>'duration' AS duration,
               settings->'features' AS features,
               (settings->>'limit')::integer AS requested_limit,
               returned_count, unique_count, new_count, rejected_count,
               estimated_credits, billed_credits
        FROM search_runs ORDER BY started_at, run_id
    """,
    "search_results.csv": """
        SELECT run_id, video_id AS id, result_rank, is_new,
               metadata->>'title' AS title,
               metadata->'channel'->>'name' AS channel,
               metadata->>'viewCount' AS "viewCount",
               metadata->>'uploadDate' AS "uploadDate",
               metadata->>'duration' AS duration
        FROM search_results ORDER BY run_id, result_rank
    """,
}


def load_settings():
    """Reject invalid settings before making a paid request."""
    settings = json.loads((ROOT / "settings.json").read_text())
    choices = {
        "type": ("video",),
        "uploadDate": ("all", "hour", "today", "week", "month", "year"),
        "duration": ("all", "short", "medium", "long"),
        "sortBy": ("relevance", "rating", "date", "views"),
    }
    if not isinstance(settings, dict) or set(settings) != {
        "query", "type", "uploadDate", "duration", "sortBy", "features", "limit"
    }:
        raise ValueError("Use exactly the seven parameters in settings.json.")
    if not isinstance(settings["query"], str) or not settings["query"].strip():
        raise ValueError("query must be a nonempty string.")
    for name, allowed in choices.items():
        if settings[name] not in allowed:
            raise ValueError(f"{name} must be one of {allowed}.")
    if type(settings["limit"]) is not int or not 1 <= settings["limit"] <= 5000:
        raise ValueError("limit must be an integer between 1 and 5000.")
    features = settings["features"]
    allowed_features = (
        "hd", "subtitles", "creative-commons", "3d", "live", "4k",
        "360", "location", "hdr", "vr180",
    )
    if not isinstance(features, list) or any(f not in allowed_features for f in features):
        raise ValueError(f"features must be a list drawn from {allowed_features}.")
    return settings


def save_checkpoint(run_id, response, billed_credits):
    """Keep the response on disk until its database transaction succeeds."""
    path = PENDING / f"{run_id}.json"
    with tempfile.NamedTemporaryFile(mode="w", dir=PENDING, suffix=".tmp", delete=False) as file:
        json.dump({"run_id": str(run_id), "response": response,
                   "billed_credits": billed_credits}, file)
        file.flush()
        os.fsync(file.fileno())
    os.replace(file.name, path)
    return path


def run_search(conn):
    """Make one request. A failed request is never automatically repeated."""
    settings = load_settings()
    api_key = os.environ.get("SUPADATA_API_KEY")
    if not api_key or not api_key.strip():
        raise ValueError("Set SUPADATA_API_KEY in the private environment.")
    PENDING.mkdir(exist_ok=True)
    if any(PENDING.glob("*.json")):
        raise RuntimeError("Resume the response in data/pending before another search.")

    run_id = database.start_run(conn, settings)
    billed_credits = None
    try:
        response = requests.get(
            "https://api.supadata.ai/v1/youtube/search",
            params=settings, headers={"x-api-key": api_key},
            timeout=(10, 180), allow_redirects=False,
        )
        billed = response.headers.get("x-billable-requests", "")
        billed_credits = int(billed) if billed.isdigit() else None
        if response.status_code != 200:
            raise RuntimeError(f"Supadata returned HTTP {response.status_code}.")
        payload = response.json()
        path = save_checkpoint(run_id, payload, billed_credits)
        counts = database.save_results(conn, run_id, payload, billed_credits)
        path.unlink()
    except Exception as error:
        try:
            database.fail_run(conn, run_id, error, billed_credits)
        except Exception:
            logging.exception("Could not record failure for run %s", run_id)
        raise
    return {"run_id": str(run_id), **counts}


def resume(conn, path):
    """Save an existing response without contacting Supadata."""
    saved = json.loads(path.read_text())
    run_id = UUID(saved["run_id"])
    counts = database.save_results(conn, run_id, saved["response"], saved["billed_credits"])
    path.unlink()
    return {"run_id": str(run_id), **counts}


def export_snapshot(conn):
    """Bundle private CSVs from the same database snapshot; no API request."""
    folder = DATA / "exports"
    folder.mkdir(exist_ok=True)
    destination = folder / "snapshot.zip"
    with tempfile.NamedTemporaryFile(dir=folder, suffix=".zip", delete=False) as file:
        temporary = Path(file.name)
        try:
            with conn.transaction(), conn.cursor() as cur:
                cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
                cur.execute("SET LOCAL TIME ZONE 'UTC'")
                with ZipFile(file, "w", compression=ZIP_DEFLATED) as archive:
                    for name, query in EXPORT_QUERIES.items():
                        with archive.open(name, "w") as output:
                            with cur.copy(f"COPY ({query}) TO STDOUT WITH CSV HEADER") as copy:
                                for chunk in copy:
                                    output.write(chunk)
            file.flush()
            os.fsync(file.fileno())
            os.replace(temporary, destination)
        finally:
            temporary.unlink(missing_ok=True)
    return {"snapshot": str(destination), "files": list(EXPORT_QUERIES)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("init", "search", "resume", "export"))
    parser.add_argument("checkpoint", nargs="?", type=Path,
                        help="Path to data/pending/<run_id>.json; required for resume")
    args = parser.parse_args()
    if (args.command == "resume") != (args.checkpoint is not None):
        parser.error("Pass a checkpoint path only with resume.")

    # Private response files and logs stay inside the ignored data directory.
    os.umask(0o077)
    DATA.mkdir(exist_ok=True)
    logging.basicConfig(filename=DATA / "pipeline.log", level=logging.INFO)
    try:
        required = ("PGHOST", "PGDATABASE", "PGUSER", "PGPASSWORD")
        missing = [name for name in required if not os.environ.get(name)]
        if missing:
            raise ValueError(f"Missing database environment variables: {', '.join(missing)}")
        with psycopg.connect(autocommit=True, connect_timeout=10) as conn:
            if args.command == "init":
                database.init_db(conn)
                print("Database tables are ready.")
            elif args.command == "search":
                print(json.dumps(run_search(conn)))
            elif args.command == "export":
                print(json.dumps(export_snapshot(conn)))
            else:
                print(json.dumps(resume(conn, args.checkpoint)))
    except Exception as error:
        # Public automation logs receive counts or a short error, never video records.
        logging.exception("Pipeline command failed")
        print(f"{type(error).__name__}: see the private log at {DATA / 'pipeline.log'}",
              file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
