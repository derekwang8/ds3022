"""
Work with Claude Code (CC) to complete this prefect flow.

The best way to do this is to open CC in the root dir of this
project, and your prompt can reference @05_prefect/07_github_api_pagination.py
to ask for a plan for how to complete the steps. the instructors
inline comments already add to the prompt - i.e. to paginate over that
API endpoint for a certain number of pages, and to store the output
in a single duckdb table. Ask CC to enumerate the steps you will need
to do this, and REQUIRE it to work with you on each task, one by one.

Read what CC generates/suggests with each step, and ask yourself if you
understand the python it has generated. IF YOU DO NOT UNDERSTAND IT WELL,
this is your chance to learn a bit more python! Ask CC to explain the specific
lines that are unclear to you, and have a back and forth with CC until you
get a sense for what is going on. You know about loops, and you know about
fetching remote web content, and so the growth here is understanding how
to cycle through a series of related links to build up all 1000 records.
"""

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
import duckdb
import httpx
import pandas as pd
from prefect import flow, task
from prefect.logging import get_run_logger
from prefect.task_runners import ThreadPoolTaskRunner


# commit history for the git/git repository
TOKEN = os.environ["GH_PAT"]
HEADERS = {"Authorization": f"Bearer {TOKEN}"}
URL = "https://api.github.com/repos/git/git/commits"
PER_PAGE = 100
PAGES = 10


COLUMNS = ["sha", "author_name", "author_date", "message", "html_url", "raw", "ingested_at"]

# local duckdb file, created if missing
DB_FILE = "actions.duckdb"

# copy every prefect log line (including print() from log_prints) to a local file
LOG_FILE = Path(__file__).with_suffix(".log")
file_handler = logging.FileHandler(LOG_FILE)
file_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s"))
logging.getLogger("prefect").addHandler(file_handler)

def load_to_duckdb(all_commits):
    """Flatten commit dicts into rows and replace the git_commits table with them."""
    # flatten each nested commit dict into a row (tuple) for the table
    ingested_at = datetime.now(timezone.utc)
    rows = [
        (
            c["sha"],
            c["commit"]["author"]["name"],
            c["commit"]["author"]["date"],
            (c["commit"]["message"].splitlines() or [""])[0],  # first line; "" if empty
            c["html_url"],
            json.dumps(c),  # keep the full original record
            ingested_at,
        )
        for c in all_commits
    ]

    conn = duckdb.connect(DB_FILE)  # opened per call so a retry gets a fresh connection
    try:
        # replace the table on each run so reruns don't duplicate rows
        conn.execute("""
            CREATE OR REPLACE TABLE git_commits (
                sha          VARCHAR PRIMARY KEY,
                author_name  VARCHAR,
                author_date  TIMESTAMPTZ,
                message      VARCHAR,
                html_url     VARCHAR,
                raw          JSON,
                ingested_at  TIMESTAMPTZ
            )
        """)
        # bulk insert: duckdb reads the local DataFrame `df` by name in one pass,
        # casting strings to the table's TIMESTAMPTZ/JSON types
        df = pd.DataFrame(rows, columns=COLUMNS)
        conn.execute("INSERT INTO git_commits SELECT * FROM df")

        count = conn.execute("SELECT COUNT(*) FROM git_commits").fetchone()[0]
        print(f"loaded {count} rows into git_commits")
    finally:
        # runs on success or failure, so the db file is never left locked
        conn.close()


# ------ serial version: one task follows the "next" links ------
@task(name="ingest", log_prints=True, retries=3, retry_delay_seconds=5)
def ingest_data():
    """Page through PAGES x PER_PAGE git/git commits and load them into one duckdb table.

    Pagination follows GitHub's Link header (response.links["next"]), as in
    04_apis/05_pagination.py. Runs show up at https://sds-prefect.pods.uvarc.io/v2/runs
    """
    logger = get_run_logger()
    try:
        # follow the "next" link page by page until it runs out or we hit PAGES
        url = URL
        params = {"per_page": PER_PAGE}  # only needed on the first request
        page = 1
        all_commits = []
        while url and page <= PAGES:
            response = httpx.get(url, headers=HEADERS, params=params, timeout=30)
            response.raise_for_status()  # 4xx/5xx raises, which triggers the task's retries
            commits = response.json()    # list of commit dicts
            all_commits.extend(commits)
            print(f"page {page}: {len(commits)} commits (total {len(all_commits)})")

            url = response.links.get("next", {}).get("url")  # None on the last page
            params = None  # the next url already carries per_page and page
            page += 1

        print(f"fetched {len(all_commits)} commits")
        load_to_duckdb(all_commits)
    except Exception:
        # log the full traceback, then re-raise so prefect marks the attempt failed and retries
        logger.exception("ingest failed")
        raise


# ------ concurrent version: one task per page, fanned out with .map() ------
@task(name="fetch-page", log_prints=True, retries=3, retry_delay_seconds=5)
def fetch_page(page):
    """Fetch a single page by number; pages are independent, so they can run at once."""
    logger = get_run_logger()
    try:
        # ask for the page directly - we can't follow "next" links without going in order
        response = httpx.get(URL, headers=HEADERS, params={"per_page": PER_PAGE, "page": page}, timeout=30)
        response.raise_for_status()
        commits = response.json()
        print(f"page {page}: {len(commits)} commits")
        return commits
    except Exception:
        logger.exception(f"fetch of page {page} failed")
        raise


@task(name="load", log_prints=True)
def load_commits(pages):
    """Combine the list of pages (each a list of commits) and load them in one write."""
    logger = get_run_logger()
    try:
        all_commits = [c for page in pages for c in page]  # flatten list of lists
        print(f"fetched {len(all_commits)} commits across {len(pages)} pages")
        load_to_duckdb(all_commits)
    except Exception:
        logger.exception("load failed")
        raise


# name the flow something you can identify as yourself.
# the thread pool lets up to PAGES tasks run at the same time.
@flow(name="ant8zr-flow", task_runner=ThreadPoolTaskRunner(max_workers=PAGES))
def main_flow(concurrent: bool = True):
    if concurrent:
        # .map() submits fetch_page(1)..fetch_page(10) at once; .result() waits for all,
        # returning the pages in the same order they were submitted
        pages = fetch_page.map(range(1, PAGES + 1)).result()
        load_commits(pages)
    else:
        ingest_data()

# defining main_flow() doesn't run it, so something has to call it.
# this guard calls it only when the file is run directly (python 07_...py),
# not when another module imports it.
if __name__ == "__main__":
    main_flow()
