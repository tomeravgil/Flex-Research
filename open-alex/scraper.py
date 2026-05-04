import requests
import time

BASE_URL = "https://api.openalex.org"
EMAIL = "your@email.com"
HEADERS = {"User-Agent": f"mailto:{EMAIL}"}


def paginate(endpoint, params=None):
    params = params or {}
    params["per_page"] = 100
    params["cursor"] = "*"

    while True:
        url = f"{BASE_URL}/{endpoint}"
        response = requests.get(url, params=params, headers=HEADERS)
        response.raise_for_status()
        data = response.json()

        yield from data["results"]

        next_cursor = data["meta"].get("next_cursor")
        if not next_cursor:
            break

        params["cursor"] = next_cursor
        time.sleep(0.1)


def oa_id(url_or_str):
    """Extract raw OpenAlex ID string e.g. 'S123456' — kept for reference only."""
    if not url_or_str:
        return None
    return str(url_or_str).split("/")[-1]


def escape(val):
    if val is None:
        return "NULL"
    if isinstance(val, int):
        return str(val)
    return "'" + str(val).replace("'", "''") + "'"


# ---------------------------------------------------------------------------
# Domains
# ---------------------------------------------------------------------------

def scrape_domains():
    rows = []
    for i, d in enumerate(paginate("domains"), start=1):
        rows.append((i, oa_id(d["id"]), d["display_name"]))
    print(f"Fetched {len(rows)} domains")
    return rows


def dump_domains(rows):
    lines = [
        "CREATE TABLE IF NOT EXISTS Domain (",
        "    id INTEGER PRIMARY KEY,",
        "    name TEXT NOT NULL",
        ");",
        "",
    ]
    for id_, oa, name in rows:
        lines.append(f"INSERT INTO Domain (id, name) VALUES ({id_}, {escape(name)});")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Fields
# ---------------------------------------------------------------------------

def scrape_fields(domain_map):
    rows = []
    for i, f in enumerate(paginate("fields"), start=1):
        d_id = domain_map.get(oa_id((f.get("domain") or {}).get("id")))
        rows.append((i, oa_id(f["id"]), f["display_name"], d_id))
    print(f"Fetched {len(rows)} fields")
    return rows


def dump_fields(rows):
    lines = [
        "CREATE TABLE IF NOT EXISTS Field (",
        "    id INTEGER PRIMARY KEY,",
        "    name TEXT NOT NULL,",
        "    domain_id INTEGER",
        ");",
        "",
    ]
    for id_, oa, name, domain_id in rows:
        lines.append(
            f"INSERT INTO Field (id, name, domain_id) VALUES "
            f"({id_}, {escape(name)}, {escape(domain_id)});"
        )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Subfields
# ---------------------------------------------------------------------------

def scrape_subfields(domain_map, field_map):
    rows = []
    for i, sf in enumerate(paginate("subfields"), start=1):
        f_id = field_map.get(oa_id((sf.get("field")  or {}).get("id")))
        d_id = domain_map.get(oa_id((sf.get("domain") or {}).get("id")))
        rows.append((i, oa_id(sf["id"]), sf["display_name"], f_id, d_id))
    print(f"Fetched {len(rows)} subfields")
    return rows


def dump_subfields(rows):
    lines = [
        "CREATE TABLE IF NOT EXISTS SubField (",
        "    id INTEGER PRIMARY KEY,",
        "    name TEXT NOT NULL,",
        "    field_id INTEGER,",
        "    domain_id INTEGER",
        ");",
        "",
    ]
    for id_, oa, name, field_id, domain_id in rows:
        lines.append(
            f"INSERT INTO SubField (id, name, field_id, domain_id) VALUES "
            f"({id_}, {escape(name)}, {escape(field_id)}, {escape(domain_id)});"
        )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Conferences
# ---------------------------------------------------------------------------

def scrape_conferences(domain_map, field_map, subfield_map):
    rows = []
    for i, src in enumerate(paginate("sources", params={"filter": "type:conference"}), start=1):
        topics = src.get("topics") or []
        sf_id = f_id = d_id = None
        if topics:
            top   = topics[0]
            sf_id = subfield_map.get(oa_id((top.get("subfield") or {}).get("id")))
            f_id  = field_map.get(oa_id((top.get("field")       or {}).get("id")))
            d_id  = domain_map.get(oa_id((top.get("domain")     or {}).get("id")))

        h_index = (src.get("summary_stats") or {}).get("h_index")
        rows.append((i, oa_id(src["id"]), src["display_name"], sf_id, f_id, d_id, h_index))
    print(f"Fetched {len(rows)} conferences")
    return rows


def dump_conferences(rows):
    lines = [
        "CREATE TABLE IF NOT EXISTS Conference (",
        "    id INTEGER PRIMARY KEY,",
        "    name TEXT NOT NULL,",
        "    subfield_id INTEGER,",
        "    field_id INTEGER,",
        "    domain_id INTEGER,",
        "    h_index INTEGER",
        ");",
        "",
    ]
    for id_, oa, name, sf_id, f_id, d_id, h_index in rows:
        lines.append(
            f"INSERT INTO Conference (id, name, subfield_id, field_id, domain_id, h_index) VALUES "
            f"({id_}, {escape(name)}, {escape(sf_id)}, {escape(f_id)}, {escape(d_id)}, {escape(h_index)});"
        )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def save(filename, content):
    with open(filename, "w") as f:
        f.write(content)
    print(f"Saved {filename}")


if __name__ == "__main__":
    # Scrape domains first — build lookup maps for FK resolution
    domain_rows = scrape_domains()
    domain_map  = {row[1]: row[0] for row in domain_rows}  # openalex_id -> our int id

    field_rows = scrape_fields(domain_map)
    field_map  = {row[1]: row[0] for row in field_rows}

    subfield_rows = scrape_subfields(domain_map, field_map)
    subfield_map  = {row[1]: row[0] for row in subfield_rows}

    conference_rows = scrape_conferences(domain_map, field_map, subfield_map)

    save("domains.sql",     dump_domains(domain_rows))
    save("fields.sql",      dump_fields(field_rows))
    save("subfields.sql",   dump_subfields(subfield_rows))
    save("conferences.sql", dump_conferences(conference_rows))