"""
Step 2: Load ArXiv (Kaggle) + OpenAlex works, match, score, and populate paper tables.

Usage:
    pip install duckdb kaggle scipy
    # Set up Kaggle API credentials first:
    #   https://www.kaggle.com/docs/api#authentication
    python3 02_load_papers.py

This script:
  1. Downloads the ArXiv metadata snapshot from Kaggle
  2. Loads ArXiv into a staging DuckDB table
  3. Syncs OpenAlex works .gz files from S3
  4. Streams each .gz file, matching works to ArXiv papers by ArXiv ID
  5. Reconstructs abstracts from OpenAlex inverted index (prefers ArXiv abstract)
  6. Populates paper_sql and paper_vdb tables
  7. Normalizes FWCI via percentile rank
  8. Computes avg_fwci per conference and back-fills missing conference fields
"""

import os
import glob
import json
import gzip
import subprocess
import duckdb
import numpy as np
from scipy.stats import rankdata
import requests
import zipfile

# ── Config ────────────────────────────────────────────────────────────────────
DB_PATH          = "./openalex.duckdb"
SNAPSHOT_DIR     = "./openalex-snapshot"
ARXIV_DIR        = "./arxiv"
ARXIV_FILE       = "./arxiv/arxiv-metadata-oai-snapshot.json"
S3_WORKS         = "s3://openalex/data/works"
WORKS_LOCAL      = os.path.join(SNAPSHOT_DIR, "data", "works")
BATCH_SIZE       = 10_000   # rows buffered before writing to DB

def download_arxiv():
    if os.path.exists(ARXIV_FILE):
        print(f"[ArXiv] Already exists: {ARXIV_FILE}, skipping download.")
        return

    token = os.environ.get("KAGGLE_API_TOKEN")
    if not token:
        raise EnvironmentError(
            "KAGGLE_API_TOKEN not set. Run: export KAGGLE_API_TOKEN=your_token"
        )

    print("[ArXiv] Downloading from Kaggle API...")
    os.makedirs(ARXIV_DIR, exist_ok=True)

    # Kaggle dataset download endpoint
    url = "https://www.kaggle.com/api/v1/datasets/download/Cornell-University/arxiv"
    headers = {"Authorization": f"Bearer {token}"}

    response = requests.get(url, headers=headers, stream=True)
    if response.status_code != 200:
        raise RuntimeError(f"Kaggle API error: {response.status_code} {response.text}")

    zip_path = os.path.join(ARXIV_DIR, "arxiv.zip")
    total = int(response.headers.get("content-length", 0))
    downloaded = 0

    with open(zip_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            f.write(chunk)
            downloaded += len(chunk)
            if total:
                pct = downloaded / total * 100
                print(f"\r[ArXiv] Downloading... {pct:.1f}%", end="", flush=True)

    print("\n[ArXiv] Extracting...")
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(ARXIV_DIR)
    os.remove(zip_path)
    print("[ArXiv] Done.")


# ── Step 2: Load ArXiv into DuckDB ───────────────────────────────────────────
def load_arxiv(con: duckdb.DuckDBPyConnection):
    print("\n[DB] Loading ArXiv into staging table...")
    con.execute("DROP TABLE IF EXISTS arxiv_staging")
    con.execute(f"""
        CREATE TABLE arxiv_staging AS
        SELECT
            id                          AS arxiv_id,
            title,
            abstract,
            authors                     AS authors_raw
        FROM read_ndjson_auto('{ARXIV_FILE}', ignore_errors=true)
        WHERE id IS NOT NULL
    """)
    count = con.execute("SELECT COUNT(*) FROM arxiv_staging").fetchone()[0]
    print(f"[DB] arxiv_staging: {count:,} rows loaded")
    con.execute("CREATE INDEX IF NOT EXISTS idx_arxiv_id ON arxiv_staging(arxiv_id)")


# ── Step 3: Sync OpenAlex works from S3 ──────────────────────────────────────
def sync_works():
    if glob.glob(os.path.join(WORKS_LOCAL, "**", "*.gz"), recursive=True):
        print(f"[S3] Works already present at {WORKS_LOCAL}, skipping sync.")
        return

    print("[S3] Syncing OpenAlex works (~250GB, this will take a while)...")
    os.makedirs(WORKS_LOCAL, exist_ok=True)
    subprocess.run([
        "aws", "s3", "sync",
        S3_WORKS, WORKS_LOCAL,
        "--no-sign-request",
        "--quiet"
    ], check=True)
    print("[S3] Works sync complete.")


# ── Step 4: Reconstruct abstract from OpenAlex inverted index ─────────────────
def reconstruct_abstract(inverted_index: dict | None) -> str | None:
    if not inverted_index:
        return None
    try:
        positions = {}
        for word, pos_list in inverted_index.items():
            for pos in pos_list:
                positions[pos] = word
        return " ".join(positions[i] for i in sorted(positions))
    except Exception:
        return None


# ── Step 5: Stream works .gz files and match to ArXiv ────────────────────────
def process_works(con: duckdb.DuckDBPyConnection):
    print("\n[DB] Creating paper_raw table for matched works...")
    con.execute("DROP TABLE IF EXISTS paper_raw")
    con.execute("""
        CREATE TABLE paper_raw (
            arxiv_id        VARCHAR,
            openalex_id     VARCHAR,
            name            VARCHAR,
            abstract        VARCHAR,
            fwci            DOUBLE,
            authors         VARCHAR,   -- comma-separated names
            conference_id   VARCHAR
        )
    """)

    # Load ArXiv IDs into a Python set for fast in-memory lookup
    print("[Match] Loading ArXiv IDs into memory for matching...")
    arxiv_rows = con.execute(
        "SELECT arxiv_id, title, abstract FROM arxiv_staging"
    ).fetchall()
    arxiv_map = {row[0]: {"title": row[1], "abstract": row[2]} for row in arxiv_rows}
    print(f"[Match] {len(arxiv_map):,} ArXiv papers loaded into memory")

    gz_files = sorted(glob.glob(
        os.path.join(WORKS_LOCAL, "**", "*.gz"), recursive=True
    ))
    print(f"[Match] Found {len(gz_files)} .gz files to process\n")

    total_matched = 0
    batch = []

    for file_idx, gz_path in enumerate(gz_files):
        file_matches = 0
        try:
            with gzip.open(gz_path, "rt", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        work = json.loads(line)
                    except json.JSONDecodeError:
                        continue

                    # Extract ArXiv ID from the ids object
                    ids = work.get("ids") or {}
                    arxiv_url = ids.get("arxiv")
                    if not arxiv_url:
                        continue

                    # Normalize: "https://arxiv.org/abs/2301.00001" → "2301.00001"
                    arxiv_id = arxiv_url.replace("https://arxiv.org/abs/", "").strip()
                    if arxiv_id not in arxiv_map:
                        continue

                    # Abstract: prefer ArXiv's plain text over reconstructed
                    arxiv_abstract = arxiv_map[arxiv_id]["abstract"]
                    oa_abstract = reconstruct_abstract(
                        work.get("abstract_inverted_index")
                    )
                    abstract = arxiv_abstract or oa_abstract

                    # Authors: comma-separated display names
                    authorships = work.get("authorships") or []
                    authors = ", ".join(
                        a.get("author", {}).get("display_name", "")
                        for a in authorships
                        if a.get("author", {}).get("display_name")
                    ) or None

                    # Conference: from primary_location source
                    primary = work.get("primary_location") or {}
                    source = primary.get("source") or {}
                    conference_id = source.get("id") if source.get("type") == "conference" else None

                    batch.append((
                        arxiv_id,
                        work.get("id"),
                        work.get("title") or arxiv_map[arxiv_id]["title"],
                        abstract,
                        work.get("fwci"),
                        authors,
                        conference_id,
                    ))
                    file_matches += 1

                    if len(batch) >= BATCH_SIZE:
                        _flush_batch(con, batch)
                        total_matched += len(batch)
                        batch = []

        except Exception as e:
            print(f"  [WARN] Error reading {gz_path}: {e}")
            continue

        print(
            f"  [{file_idx+1}/{len(gz_files)}] {os.path.basename(gz_path)}"
            f" → {file_matches} matches (total: {total_matched:,})"
        )

    # Flush remaining
    if batch:
        _flush_batch(con, batch)
        total_matched += len(batch)

    print(f"\n[Match] Total matched papers: {total_matched:,}")


def _flush_batch(con: duckdb.DuckDBPyConnection, batch: list):
    con.executemany("""
        INSERT INTO paper_raw VALUES (?, ?, ?, ?, ?, ?, ?)
    """, batch)


# ── Step 6: Normalize FWCI via percentile rank ───────────────────────────────
def normalize_fwci(con: duckdb.DuckDBPyConnection):
    print("\n[DB] Normalizing FWCI via percentile rank...")

    rows = con.execute(
        "SELECT rowid, fwci FROM paper_raw WHERE fwci IS NOT NULL"
    ).fetchall()

    if not rows:
        print("[DB] No FWCI values found, skipping normalization.")
        return

    rowids = [r[0] for r in rows]
    fwci_vals = np.array([r[1] for r in rows], dtype=float)

    # Percentile rank → 0 to 1
    ranks = rankdata(fwci_vals, method="average")
    normalized = (ranks - 1) / (len(ranks) - 1) if len(ranks) > 1 else ranks * 0

    con.execute("ALTER TABLE paper_raw ADD COLUMN IF NOT EXISTS fwci_norm DOUBLE")

    update_batch = [
        (float(norm), int(rid))
        for norm, rid in zip(normalized, rowids)
    ]
    con.executemany(
        "UPDATE paper_raw SET fwci_norm = ? WHERE rowid = ?",
        update_batch
    )
    print(f"[DB] Normalized FWCI for {len(rows):,} papers")


# ── Step 7: Build final paper_sql and paper_vdb tables ───────────────────────
def build_final_tables(con: duckdb.DuckDBPyConnection):
    print("\n[DB] Building paper_sql table...")
    con.execute("DROP TABLE IF EXISTS paper_sql")
    con.execute("""
        CREATE TABLE paper_sql AS
        SELECT
            row_number() OVER ()        AS id,
            name,
            authors,
            COALESCE(fwci_norm, NULL)   AS fwci,
            conference_id,
            arxiv_id,
            openalex_id
        FROM paper_raw
    """)
    count = con.execute("SELECT COUNT(*) FROM paper_sql").fetchone()[0]
    print(f"[DB] paper_sql: {count:,} rows")

    print("\n[DB] Building paper_vdb table...")
    con.execute("DROP TABLE IF EXISTS paper_vdb")
    con.execute("""
        CREATE TABLE paper_vdb AS
        SELECT
            id      AS sqlid,
            name,
            abstract,
            authors
        FROM paper_sql
        JOIN paper_raw USING (arxiv_id)
    """)
    count = con.execute("SELECT COUNT(*) FROM paper_vdb").fetchone()[0]
    print(f"[DB] paper_vdb: {count:,} rows")


# ── Step 8: Compute avg_fwci per conference ───────────────────────────────────
def compute_conference_avg_fwci(con: duckdb.DuckDBPyConnection):
    print("\n[DB] Computing avg_fwci per conference...")
    con.execute("""
        UPDATE conference
        SET avg_fwci = (
            SELECT AVG(p.fwci)
            FROM paper_sql p
            WHERE p.conference_id = conference.id
              AND p.fwci IS NOT NULL
        )
    """)
    updated = con.execute(
        "SELECT COUNT(*) FROM conference WHERE avg_fwci IS NOT NULL"
    ).fetchone()[0]
    print(f"[DB] avg_fwci populated for {updated:,} conferences")


# ── Step 9: Back-fill conference field/subfield/domain from their papers ──────
def backfill_conference_fields(con: duckdb.DuckDBPyConnection):
    print("\n[DB] Back-filling conference field/subfield/domain from papers...")

    # For conferences with no field, find the most common field among their papers
    con.execute("""
        UPDATE conference
        SET
            field_id    = inferred.field_id,
            subfield_id = inferred.subfield_id,
            domain_id   = inferred.domain_id
        FROM (
            SELECT
                p.conference_id,
                f.id        AS field_id,
                sf.id       AS subfield_id,
                d.id        AS domain_id,
                COUNT(*)    AS cnt
            FROM paper_sql p
            -- OpenAlex works don't carry field directly; we join via conference's
            -- existing papers that DO have a field on their source
            JOIN conference c ON c.id = p.conference_id
            LEFT JOIN field f    ON f.id = c.field_id
            LEFT JOIN subfield sf ON sf.id = c.subfield_id
            LEFT JOIN domain d   ON d.id = c.domain_id
            WHERE c.field_id IS NOT NULL
            GROUP BY p.conference_id, f.id, sf.id, d.id
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY p.conference_id ORDER BY cnt DESC
            ) = 1
        ) inferred
        WHERE conference.id = inferred.conference_id
          AND conference.field_id IS NULL
    """)

    still_missing = con.execute(
        "SELECT COUNT(*) FROM conference WHERE field_id IS NULL"
    ).fetchone()[0]
    print(f"[DB] Conferences still missing field after back-fill: {still_missing}")


# ── Sanity check ──────────────────────────────────────────────────────────────
def sanity_check(con: duckdb.DuckDBPyConnection):
    print("\n─── Sanity Check ───────────────────────────────")

    print("\n[paper_sql] Sample:")
    print(con.execute("SELECT * FROM paper_sql LIMIT 5").df().to_string(index=False))

    print("\n[paper_vdb] Sample:")
    print(con.execute("SELECT * FROM paper_vdb LIMIT 3").df().to_string(index=False))

    print("\n[conference] avg_fwci sample:")
    print(con.execute("""
        SELECT name, avg_fwci, field_id
        FROM conference
        WHERE avg_fwci IS NOT NULL
        LIMIT 5
    """).df().to_string(index=False))

    print("\n[FWCI distribution]:")
    print(con.execute("""
        SELECT
            MIN(fwci)   AS min_fwci,
            MAX(fwci)   AS max_fwci,
            AVG(fwci)   AS avg_fwci,
            COUNT(*)    AS papers_with_fwci
        FROM paper_sql
        WHERE fwci IS NOT NULL
    """).df().to_string(index=False))


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    download_arxiv()

    con = duckdb.connect(DB_PATH)
    print(f"[DB] Connected to {DB_PATH}")

    load_arxiv(con)
    sync_works()
    process_works(con)
    normalize_fwci(con)
    build_final_tables(con)
    compute_conference_avg_fwci(con)
    backfill_conference_fields(con)
    sanity_check(con)

    con.close()
    print(f"\n✅ Done. Database saved to: {DB_PATH}")
    print("   Next step: embed paper_vdb abstracts and load into your vector DB.")


if __name__ == "__main__":
    main()