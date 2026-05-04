import json
import os
import time
import threading
import requests
import duckdb
from concurrent.futures import ThreadPoolExecutor, as_completed
from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams, Distance, PointStruct
from sentence_transformers import SentenceTransformer

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

ARXIV_PATH   = "../kaggle/data/arxiv-metadata-oai-snapshot.json"
DUCKDB_PATH  = "./db.duckdb"
QDRANT_PATH  = "./qdrant_db"
API_KEY      = "rvDj6CT7tFGThLsTY3RceB"
BATCH_SIZE   = 50    # papers read from file per cycle
WORKERS      = 5     # conservative — backoff handles bursts
VECTOR_DIM   = 1024  # bge-large-en-v1.5

# ---------------------------------------------------------------------------
# Init
# ---------------------------------------------------------------------------

db     = duckdb.connect(DUCKDB_PATH)
qdrant = QdrantClient(path=QDRANT_PATH)
model  = SentenceTransformer("BAAI/bge-large-en-v1.5")

# Locks for thread-safe shared state
db_lock    = threading.Lock()
cache_lock = threading.Lock()
id_lock    = threading.Lock()

# ---------------------------------------------------------------------------
# Ensure Paper table exists
# ---------------------------------------------------------------------------

db.execute("CREATE SEQUENCE IF NOT EXISTS paper_id_seq START 1")
db.execute("""
    CREATE TABLE IF NOT EXISTS Paper (
        id            INTEGER PRIMARY KEY DEFAULT nextval('paper_id_seq'),
        arxiv_id      TEXT UNIQUE,
        title         TEXT,
        conference_id INTEGER,
        updated_at    TIMESTAMP,
        created_at    TIMESTAMP DEFAULT now()
    )
""")

# ---------------------------------------------------------------------------
# Ensure Qdrant collection exists
# ---------------------------------------------------------------------------

existing = [c.name for c in qdrant.get_collections().collections]
if "paper_collection" not in existing:
    qdrant.create_collection(
        collection_name="paper_collection",
        vectors_config=VectorParams(size=VECTOR_DIM, distance=Distance.COSINE)
    )
    print("Created Qdrant collection: paper_collection")

# ---------------------------------------------------------------------------
# Conference name cache
# ---------------------------------------------------------------------------

conference_cache = {}

def lookup_conference_by_name(venue_name):
    if not venue_name:
        return None
    key = venue_name.lower().strip()
    with cache_lock:
        if key in conference_cache:
            return conference_cache[key]
    with db_lock:
        row = db.execute(
            "SELECT id FROM Conference WHERE lower(name) = ?", [key]
        ).fetchone()
    result = row[0] if row else None
    with cache_lock:
        conference_cache[key] = result
    return result

# ---------------------------------------------------------------------------
# OpenAlex lookup — one request per paper, runs in thread pool
# ---------------------------------------------------------------------------

session = requests.Session()  # reuse TCP connections across threads

def get_venue_for_title(title):
    """Fetch venue name from OpenAlex for a single title. Thread-safe with backoff."""
    backoff = 1.0
    for attempt in range(5):
        try:
            response = session.get(
                "https://api.openalex.org/works",
                params={
                    "search":   title,
                    "per_page": 1,
                    "select":   "title,primary_location",
                    "api_key":  API_KEY,
                },
                timeout=15
            )
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", backoff))
                print(f"  429 — waiting {retry_after}s (attempt {attempt + 1})")
                time.sleep(retry_after)
                backoff *= 2
                continue
            response.raise_for_status()
            results = response.json().get("results", [])
            if not results:
                return None
            source = (results[0].get("primary_location") or {}).get("source")
            return source.get("display_name") if source else None
        except Exception as e:
            print(f"  OpenAlex error for '{title[:50]}': {e}")
            time.sleep(backoff)
            backoff *= 2
    return None

# ---------------------------------------------------------------------------
# Per-paper processing — runs in thread pool
# ---------------------------------------------------------------------------

vector_counter = [1]   # list so threads can mutate via reference
paper_count    = [0]

def process_paper(paper_tuple):
    """Fetch venue, insert into DuckDB, build embedding. Returns PointStruct."""
    arxiv_id, title, abstract, updated = paper_tuple

    venue_name    = get_venue_for_title(title)
    conference_id = lookup_conference_by_name(venue_name)

    with db_lock:
        db.execute("""
            INSERT INTO Paper (arxiv_id, title, conference_id, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (arxiv_id) DO NOTHING
        """, [arxiv_id, title, conference_id, updated])
        sql_id = db.execute(
            "SELECT id FROM Paper WHERE arxiv_id = ?", [arxiv_id]
        ).fetchone()[0]

    embedding = model.encode(
        f"Represent this sentence for searching relevant passages: {title} [SEP] {abstract}"
    ).tolist()

    with id_lock:
        vid = vector_counter[0]
        vector_counter[0] += 1

    return PointStruct(
        id      = vid,
        vector  = embedding,
        payload = {"sql_id": sql_id, "arxiv_id": arxiv_id, "title": title}
    )

# ---------------------------------------------------------------------------
# Main ingestion loop
# ---------------------------------------------------------------------------

def already_ingested(arxiv_id):
    with db_lock:
        return db.execute(
            "SELECT 1 FROM Paper WHERE arxiv_id = ?", [arxiv_id]
        ).fetchone() is not None


print("Starting ingestion...")

batch = []

with open(ARXIV_PATH, "r") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue

        paper    = json.loads(line)
        title    = (paper.get("title")    or "").replace("\n", " ").strip()
        abstract = (paper.get("abstract") or "").replace("\n", " ").strip()
        arxiv_id = paper.get("id")
        updated  = paper.get("update_date")

        if not title or not abstract:
            continue
        if already_ingested(arxiv_id):
            continue

        batch.append((arxiv_id, title, abstract, updated))

        if len(batch) >= BATCH_SIZE:
            with ThreadPoolExecutor(max_workers=WORKERS) as executor:
                futures = [executor.submit(process_paper, p) for p in batch]
                points  = [f.result() for f in as_completed(futures)]

            qdrant.upsert(collection_name="paper_collection", wait=True, points=points)
            paper_count[0] += len(points)
            print(f"  Ingested {paper_count[0]} papers...")
            batch = []

# Flush remainder
if batch:
    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = [executor.submit(process_paper, p) for p in batch]
        points  = [f.result() for f in as_completed(futures)]
    qdrant.upsert(collection_name="paper_collection", wait=True, points=points)
    paper_count[0] += len(points)

db.close()
print(f"Done. Total papers ingested: {paper_count[0]}")