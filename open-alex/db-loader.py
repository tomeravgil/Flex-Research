#!/usr/bin/env python3
"""
DuckDB Database Manager
- Create a new DuckDB database
- Load SQL dump files into an existing or new database
"""

import argparse
import sys
from pathlib import Path

try:
    import duckdb
except ImportError:
    print("Error: duckdb is not installed. Run: pip install duckdb", file=sys.stderr)
    sys.exit(1)


def create_database(db_path: str) -> duckdb.DuckDBPyConnection:
    """Create (or connect to) a DuckDB database at the given path."""
    db_path = Path(db_path)
    existed = db_path.exists()
    conn = duckdb.connect(str(db_path))
    if existed:
        print(f"Connected to existing database: {db_path}")
    else:
        print(f"Created new database: {db_path}")
    return conn


def split_sql_statements(sql: str) -> list[str]:
    """
    Split a SQL script into individual statements, correctly ignoring
    semicolons that appear inside single-quoted strings.
    Handles escaped quotes ('') and skips -- line comments.
    """
    statements = []
    current: list[str] = []
    in_string = False
    i = 0

    while i < len(sql):
        ch = sql[i]

        if in_string:
            current.append(ch)
            if ch == "'":
                # Escaped quote '' inside a string — stay in string
                if i + 1 < len(sql) and sql[i + 1] == "'":
                    current.append(sql[i + 1])
                    i += 2
                    continue
                else:
                    in_string = False
        else:
            if ch == "'":
                in_string = True
                current.append(ch)
            elif ch == "-" and i + 1 < len(sql) and sql[i + 1] == "-":
                # Line comment: skip to end of line
                while i < len(sql) and sql[i] != "\n":
                    i += 1
                continue
            elif ch == ";":
                stmt = "".join(current).strip()
                if stmt:
                    statements.append(stmt)
                current = []
            else:
                current.append(ch)

        i += 1

    # Catch any trailing statement without a final semicolon
    stmt = "".join(current).strip()
    if stmt:
        statements.append(stmt)

    return statements


def load_sql_dump(conn: duckdb.DuckDBPyConnection, dump_path: str) -> None:
    """Load a SQL dump file into the connected database."""
    dump_path = Path(dump_path)

    if not dump_path.exists():
        print(f"Error: Dump file not found: {dump_path}", file=sys.stderr)
        sys.exit(1)

    print(f"Loading SQL dump: {dump_path}")
    try:
        sql = dump_path.read_text(encoding="utf-8")
        statements = split_sql_statements(sql)
        for stmt in statements:
            conn.execute(stmt)
        print(f"Dump loaded successfully ({len(statements)} statements executed).")
    except duckdb.Error as e:
        print(f"DuckDB error while loading dump: {e}", file=sys.stderr)
        sys.exit(1)
    except UnicodeDecodeError as e:
        print(f"Encoding error reading dump file: {e}", file=sys.stderr)
        sys.exit(1)


def list_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Print all tables in the database."""
    tables = conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' ORDER BY table_name;"
    ).fetchall()
    tables = [row[0] for row in tables]

    if tables:
        print(f"\nTables in database ({len(tables)}):")
        for table in tables:
            row_count = conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
            print(f"  - {table}  ({row_count} rows)")
    else:
        print("\nNo tables found in database.")


def interactive_shell(conn: duckdb.DuckDBPyConnection) -> None:
    """Simple interactive SQL shell."""
    print("\nEntering interactive SQL shell. Type 'exit' or 'quit' to leave.\n")
    while True:
        try:
            query = input("duckdb> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nExiting shell.")
            break

        if not query:
            continue
        if query.lower() in ("exit", "quit"):
            print("Exiting shell.")
            break

        try:
            result = conn.execute(query)
            rows = result.fetchall()
            if result.description:
                headers = [desc[0] for desc in result.description]
                col_widths = [
                    max(len(h), max((len(str(r[i])) for r in rows), default=0))
                    for i, h in enumerate(headers)
                ]
                header_line = " | ".join(h.ljust(col_widths[i]) for i, h in enumerate(headers))
                separator = "-+-".join("-" * w for w in col_widths)
                print(header_line)
                print(separator)
                for row in rows:
                    print(" | ".join(str(v).ljust(col_widths[i]) for i, v in enumerate(row)))
                print(f"\n({len(rows)} rows)\n")
            else:
                print("OK.\n")
        except duckdb.Error as e:
            print(f"Error: {e}\n")


def main():
    parser = argparse.ArgumentParser(
        description="DuckDB Database Manager — create databases and load SQL dumps."
    )
    parser.add_argument(
        "database",
        help="Path to the DuckDB database file (created if it doesn't exist)."
    )
    parser.add_argument(
        "--load", "-l",
        metavar="DUMP_FILE",
        action="append",
        default=[],
        help="SQL dump file to load. Can be specified multiple times."
    )
    parser.add_argument(
        "--list-tables", "-t",
        action="store_true",
        help="List all tables and row counts after setup."
    )
    parser.add_argument(
        "--shell", "-s",
        action="store_true",
        help="Start an interactive SQL shell after setup."
    )

    args = parser.parse_args()

    conn = create_database(args.database)

    for dump_file in args.load:
        load_sql_dump(conn, dump_file)

    if args.list_tables or args.load:
        list_tables(conn)

    if args.shell:
        interactive_shell(conn)

    conn.close()
    print("\nDone. Database connection closed.")


if __name__ == "__main__":
    main()