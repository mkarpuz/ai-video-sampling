"""Store the video catalogue and search history in PostgreSQL."""

from uuid import uuid4

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb


WORKER_LOCK = 752041004


class BudgetExceeded(RuntimeError):
    """The next request would exceed the monthly discovery allowance."""


SCHEMA = (
    """
    CREATE TABLE IF NOT EXISTS search_runs (
        run_id UUID PRIMARY KEY,
        started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        finished_at TIMESTAMPTZ,
        settings JSONB NOT NULL,
        status TEXT NOT NULL DEFAULT 'running'
            CHECK (status IN ('running', 'complete', 'failed')),
        estimated_credits INTEGER NOT NULL,
        billed_credits INTEGER,
        returned_count INTEGER,
        unique_count INTEGER,
        new_count INTEGER,
        rejected_count INTEGER,
        response JSONB,
        error TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS videos (
        video_id TEXT PRIMARY KEY CHECK (length(video_id) > 0),
        first_seen_at TIMESTAMPTZ NOT NULL,
        last_seen_at TIMESTAMPTZ NOT NULL,
        metadata JSONB NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS search_results (
        run_id UUID NOT NULL REFERENCES search_runs(run_id),
        video_id TEXT NOT NULL REFERENCES videos(video_id),
        result_rank INTEGER NOT NULL CHECK (result_rank > 0),
        is_new BOOLEAN NOT NULL,
        metadata JSONB NOT NULL,
        PRIMARY KEY (run_id, video_id)
    )
    """,
    """ALTER TABLE search_runs
       ADD COLUMN IF NOT EXISTS strategy TEXT,
       ADD COLUMN IF NOT EXISTS scheduled_for DATE""",
    """CREATE UNIQUE INDEX IF NOT EXISTS search_runs_strategy_date
       ON search_runs (strategy, scheduled_for)""",
)


def init_db(conn):
    """Create the three tables. Existing records are preserved."""
    with conn.transaction():
        for statement in SCHEMA:
            conn.execute(statement)


def monthly_usage(conn, timezone):
    """Count requests conservatively within the local calendar month."""
    with conn.cursor(row_factory=dict_row) as cur:
        return cur.execute(
            """WITH period AS (
                   SELECT date_trunc('month', now() AT TIME ZONE %s) AS start
               )
               SELECT COALESCE(sum(GREATEST(estimated_credits,
                      COALESCE(billed_credits, 0))), 0) AS credits
               FROM search_runs, period
               WHERE started_at >= (period.start AT TIME ZONE %s)
                 AND started_at < ((period.start + interval '1 month') AT TIME ZONE %s)""",
            (timezone, timezone, timezone),
        ).fetchone()["credits"]


def scheduled_run(conn, strategy, day):
    """Find an existing attempt for one strategy and date."""
    with conn.cursor(row_factory=dict_row) as cur:
        return cur.execute(
            "SELECT run_id, status FROM search_runs WHERE strategy = %s AND scheduled_for = %s",
            (strategy, day),
        ).fetchone()


def start_run(conn, settings, *, monthly_cap, timezone, strategy=None, scheduled_for=None):
    """Record the settings before a paid request and return its run ID."""
    run_id = uuid4()
    estimated_credits = (settings["limit"] + 19) // 20
    with conn.transaction():
        # Serialize the budget check and reservation, including manual searches.
        conn.execute("SELECT pg_advisory_xact_lock(%s)", (WORKER_LOCK,))
        if (strategy is None) != (scheduled_for is None):
            raise ValueError("A scheduled run needs both a strategy and a date.")
        if strategy is not None:
            previous = scheduled_run(conn, strategy, scheduled_for)
            if previous:
                if previous["status"] == "complete":
                    return None
                raise RuntimeError(f"Review or resume existing run {previous['run_id']}.")
        used = monthly_usage(conn, timezone)
        if used + estimated_credits > monthly_cap:
            raise BudgetExceeded(f"{used} credits reserved; next request needs {estimated_credits}; "
                                 f"monthly cap is {monthly_cap}.")
        conn.execute(
            """INSERT INTO search_runs
                   (run_id, settings, estimated_credits, strategy, scheduled_for)
               VALUES (%s, %s, %s, %s, %s)""",
            (run_id, Jsonb(settings), estimated_credits, strategy, scheduled_for),
        )
    return run_id


def save_results(conn, run_id, response, billed_credits=None):
    """Save a response once; retain the first rank for repeated IDs."""
    results = response["results"]
    if not isinstance(results, list):
        raise ValueError("Expected a list of search results.")

    unique = {}
    rejected = 0
    for rank, video in enumerate(results, start=1):
        video_id = video.get("id") if isinstance(video, dict) else None
        if (not isinstance(video_id, str) or not video_id.strip()
                or video.get("type") != "video"):
            rejected += 1
            continue
        unique.setdefault(video_id.strip(), (rank, video))

    with conn.transaction(), conn.cursor(row_factory=dict_row) as cur:
        run = cur.execute(
            "SELECT * FROM search_runs WHERE run_id = %s FOR UPDATE",
            (run_id,),
        ).fetchone()
        if run is None:
            raise ValueError("Unknown search run ID.")
        if run["status"] == "complete":
            return {name: run[name] for name in (
                "returned_count", "unique_count", "new_count", "rejected_count"
            )}

        seen_at = run["started_at"]
        new_count = 0
        for video_id, (rank, video) in unique.items():
            inserted = cur.execute(
                """INSERT INTO videos
                       (video_id, first_seen_at, last_seen_at, metadata)
                   VALUES (%s, %s, %s, %s)
                   ON CONFLICT (video_id) DO NOTHING RETURNING video_id""",
                (video_id, seen_at, seen_at, Jsonb(video)),
            ).fetchone()
            is_new = inserted is not None
            if is_new:
                new_count += 1
            else:
                cur.execute(
                    """UPDATE videos SET
                           first_seen_at = LEAST(first_seen_at, %s),
                           last_seen_at = GREATEST(last_seen_at, %s),
                           metadata = CASE WHEN last_seen_at <= %s
                               THEN %s ELSE metadata END
                       WHERE video_id = %s""",
                    (seen_at, seen_at, seen_at, Jsonb(video), video_id),
                )
            cur.execute(
                """INSERT INTO search_results
                       (run_id, video_id, result_rank, is_new, metadata)
                   VALUES (%s, %s, %s, %s, %s)""",
                (run_id, video_id, rank, is_new, Jsonb(video)),
            )

        return cur.execute(
            """UPDATE search_runs SET
                   status = 'complete', finished_at = now(), error = NULL,
                   response = %s, billed_credits = %s,
                   returned_count = %s, unique_count = %s,
                   new_count = %s, rejected_count = %s
               WHERE run_id = %s
               RETURNING returned_count, unique_count, new_count, rejected_count""",
            (Jsonb(response), billed_credits, len(results), len(unique),
             new_count, rejected, run_id),
        ).fetchone()


def fail_run(conn, run_id, error, billed_credits=None):
    """Keep a record of failed requests without changing completed runs."""
    with conn.transaction():
        conn.execute(
            """UPDATE search_runs SET status = 'failed', finished_at = now(),
                   error = %s, billed_credits = COALESCE(%s, billed_credits)
               WHERE run_id = %s AND status <> 'complete'""",
            (str(error), billed_credits, run_id),
        )
