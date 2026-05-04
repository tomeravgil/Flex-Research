"""
Step 1: Load Fields, Subfields, Domains, and Conferences from OpenAlex snapshot.

Usage:
    pip install duckdb boto3
    python 01_load_taxonomy_and_conferences.py

This script:
  1. Downloads fields, subfields, domains, topics, and sources (conferences)
     directly from the OpenAlex S3 bucket (no AWS account needed).
  2. Parses and stores them into a local DuckDB database.
  3. avg_fwci on conferences is left NULL here — it will be populated
     in the next script (02_load_papers.py) after works are processed.

Directory layout expected/created:
    openalex-snapshot/
        data/
            domains/
            fields/
            subfields/
            topics/
            sources/
    openalex.duckdb   ← output database
"""

import os
import glob
import subprocess
import duckdb

# ── Config ────────────────────────────────────────────────────────────────────
SNAPSHOT_DIR = "./openalex-snapshot"
DB_PATH = "./openalex.duckdb"
S3_BASE = "s3://openalex"

# Entity types to download for taxonomy + conferences
ENTITIES = ["domains", "fields", "subfields", "topics", "sources"]


# ── Helpers ───────────────────────────────────────────────────────────────────
def sync_entity(entity: str):
    """Download a single entity type from OpenAlex S3 (no AWS account needed)."""
    local_path = os.path.join(SNAPSHOT_DIR, "data", entity)
    os.makedirs(local_path, exist_ok=True)
    print(f"\n[S3] Syncing {entity}...")
    subprocess.run([
        "aws", "s3", "sync",
        f"{S3_BASE}/data/{entity}",
        local_path,
        "--no-sign-request",
        "--quiet"
    ], check=True)
    print(f"[S3] Done: {entity}")


def gz_files(entity: str) -> list[str]:
    """Return all .gz files for a given entity type."""
    pattern = os.path.join(SNAPSHOT_DIR, "data", entity, "**", "*.gz")
    files = glob.glob(pattern, recursive=True)
    if not files:
        raise FileNotFoundError(f"No .gz files found for entity '{entity}'. "
                                f"Did the download complete?")
    return files


def files_as_duckdb_list(entity: str) -> str:
    """Return a DuckDB-compatible list literal of .gz file paths."""
    return "[" + ", ".join(f"'{f}'" for f in gz_files(entity)) + "]"


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    # 1. Download entities from S3
    for entity in ENTITIES:
        sync_entity(entity)

    # 2. Connect to DuckDB
    con = duckdb.connect(DB_PATH)
    print(f"\n[DB] Connected to {DB_PATH}")

    # ── Domains ───────────────────────────────────────────────────────────────
    print("\n[DB] Loading domains...")
    con.execute("DROP TABLE IF EXISTS domain")
    con.execute(f"""
        CREATE TABLE domain AS
        SELECT
            id,
            display_name AS name
        FROM read_ndjson_auto({files_as_duckdb_list('domains')})
        WHERE id IS NOT NULL
    """)
    count = con.execute("SELECT COUNT(*) FROM domain").fetchone()[0]
    print(f"[DB] domain: {count} rows")

    # ── Fields ────────────────────────────────────────────────────────────────
    print("\n[DB] Loading fields...")
    con.execute("DROP TABLE IF EXISTS field")
    con.execute(f"""
        CREATE TABLE field AS
        SELECT
            id,
            display_name                    AS name,
            domain.id                       AS domain_id
        FROM read_ndjson_auto({files_as_duckdb_list('fields')})
        WHERE id IS NOT NULL
    """)
    count = con.execute("SELECT COUNT(*) FROM field").fetchone()[0]
    print(f"[DB] field: {count} rows")

    # ── Subfields ─────────────────────────────────────────────────────────────
    print("\n[DB] Loading subfields...")
    con.execute("DROP TABLE IF EXISTS subfield")
    con.execute(f"""
        CREATE TABLE subfield AS
        SELECT
            id,
            display_name                    AS name,
            field.id                        AS field_id,
            domain.id                       AS domain_id
        FROM read_ndjson_auto({files_as_duckdb_list('subfields')})
        WHERE id IS NOT NULL
    """)
    count = con.execute("SELECT COUNT(*) FROM subfield").fetchone()[0]
    print(f"[DB] subfield: {count} rows")

    # ── Conferences ───────────────────────────────────────────────────────────
    # OpenAlex sources with type='conference' are what we want.
    # Each source has a `topics` array; we take the highest-scored topic
    # to get a single field/subfield/domain for the conference.
    #
    # avg_fwci is intentionally left NULL — computed in step 02 after works.

    print("\n[DB] Loading conferences...")
    con.execute("DROP TABLE IF EXISTS conference")
    con.execute(f"""
        CREATE TABLE conference AS
        WITH raw_sources AS (
            SELECT
                id,
                display_name                                AS name,
                -- Pick the top topic by score for field/subfield/domain
                topics[1].subfield.id                       AS subfield_id,
                topics[1].field.id                          AS field_id,
                topics[1].domain.id                         AS domain_id
            FROM read_ndjson_auto({files_as_duckdb_list('sources')})
            WHERE type = 'conference'
              AND id IS NOT NULL
        )
        SELECT
            id,
            name,
            NULL::DOUBLE                                    AS avg_fwci,
            field_id,
            subfield_id,
            domain_id
        FROM raw_sources
    """)
    count = con.execute("SELECT COUNT(*) FROM conference").fetchone()[0]
    print(f"[DB] conference: {count} rows")

    # ── Sanity checks ─────────────────────────────────────────────────────────
    print("\n[DB] Sample domains:")
    print(con.execute("SELECT * FROM domain LIMIT 5").df().to_string(index=False))

    print("\n[DB] Sample fields:")
    print(con.execute("SELECT * FROM field LIMIT 5").df().to_string(index=False))

    print("\n[DB] Sample subfields:")
    print(con.execute("SELECT * FROM subfield LIMIT 5").df().to_string(index=False))

    print("\n[DB] Sample conferences:")
    print(con.execute("SELECT * FROM conference LIMIT 5").df().to_string(index=False))

    print("\n[DB] Conferences with no field assigned (may need manual review):")
    missing = con.execute(
        "SELECT COUNT(*) FROM conference WHERE field_id IS NULL"
    ).fetchone()[0]
    print(f"       {missing} conferences have no primary field")

    con.close()
    print(f"\n✅ Done. Database saved to: {DB_PATH}")
    print("   Next step: run 02_load_papers.py")


if __name__ == "__main__":
    main()