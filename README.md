# AI video sampling

A pipeline for discovering YouTube videos, tracking unique results,
selecting samples, and retrieving available transcripts.

Code, search settings, and search summaries can be public.
Collected video lists, transcripts, exports, and credentials remain private.

## Files

- `pipeline.py`: validates settings, makes searches, recovers responses,
  runs due strategies, and exports private CSV snapshots.
- `database.py`: stores unique videos, search appearances, and run history;
  reserves credits before each request.
- `settings.json`: the manual search and common defaults, including the query.
- `schedule.json`: the discovery cap, timezone, and named search strategies.
- `compose.yaml`, `Dockerfile`, `.dockerignore`: package and run Python and
  PostgreSQL, keeping collected data and credentials outside image builds.
- `Makefile`: `make sync` refreshes local private CSVs over SSH.

## Discovery plan

The approved timer time is 09:17 Europe/Berlin. Timer installation is a
separate setup step; adding this configuration does not enable it.

| Frequency | Upload filter | Sort | Limit | Credits per run |
| --- | --- | --- | ---: | ---: |
| Daily | today | relevance | 100 | 5 |
| Daily | week | date | 200 | 10 |
| Sunday | month | views | 1,000 | 50 |
| Sunday | month | relevance | 500 | 25 |

`schedule.json` accepts `daily` and `sunday` frequencies. Each strategy
overrides the corresponding values in `settings.json`. Changing a strategy
affects future runs; recorded settings and historical results are preserved.

Discovery is capped at 1,000 credits per calendar month in Europe/Berlin.
Manual and scheduled searches recorded by this pipeline share the cap.
Each attempt reserves its requested cost, including failed or unfinished
attempts. A higher reported charge is also counted. Calls made outside this
pipeline are not included.

Completed scheduled strategies run once per local date. Failed or unfinished
attempts require review or recovery; they are never automatically repeated.
Only one collection command runs at a time. No missed dates are backfilled
using today's relative upload filters.

## VPS commands

Run these from `/opt/ai-video-sampling`:

```bash
docker compose run --rm worker init
docker compose run --rm worker schedule --dry-run
docker compose run --rm worker schedule
docker compose run --rm worker search
docker compose run --rm worker resume data/pending/RUN_ID.json
```

`init` creates tables and applies additive schema changes. `--dry-run` only
previews the due strategies and their credit estimates. `schedule` and
`search` make paid requests. `resume` saves an existing response without
another request. Detailed errors stay in private `data/pipeline.log`.
Replace `RUN_ID` with the recovery file's actual run ID.

## Local access

From the Mac repository, run `make sync`, then reload the CSVs in Jupyter:

- `csv/videos.csv`: one row per unique video, with latest observed metadata.
- `csv/search_history.csv`: settings, timestamps, counts, credits and strategy.
- `csv/search_results.csv`: one appearance per video per search, with rank
  and metadata as observed during that search.

Refreshes replace analysis snapshots; PostgreSQL retains the full history.
Google Sheets summaries, GitHub Actions deployment, sample selection,
transcript collection, and private database backups remain to be added.
