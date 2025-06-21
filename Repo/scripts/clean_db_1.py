#!/usr/bin/env python3
"""Chunked removal of unwanted rows from `wikidata_labeled.db` by `pid`, with counts and timing."""
import sqlite3
import time
from pathlib import Path
import argparse

# scripts/ -> Repo/ -> Text-Embeddings/ -> WikiData.nosync/
BASE = Path(__file__).resolve().parent.parent.parent / "WikiData.nosync"
DEFAULT_DB = BASE / "wikidata_labeled.db"

TARGET_PROPERTIES = {
    "core#altLabel",
    "core#prefLabel",
    "rdf-schema#label",
    "birth name",
    "name",
    "dateModified",
}

CHUNK_SIZE = 100_000  # rows per batch delete


def clean_database(db_path: Path = DEFAULT_DB) -> None:
    """Delete rows based on unwanted `pid` values, in chunks, with logging."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")

    print(f"🔍 Connected to database: {db_path}")
    # Ensure index for fast lookups
    print("⚙️  Ensuring idx_pl_pid exists…")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pl_pid ON properties_labeled(pid);")
    conn.commit()
    print("✅ Index ready. Starting cleanup.\n" + "-" * 60)

    total_start = time.time()
    removed_grand_total = 0

    for pid in TARGET_PROPERTIES:
        # count how many rows match
        cur = conn.execute(
            "SELECT COUNT(*) FROM properties_labeled WHERE pid = ?", (pid,)
        )
        total = cur.fetchone()[0]
        print(f"PID={pid!r}: {total} rows to delete.")
        if total == 0:
            print("-" * 60)
            continue

        pid_start = time.time()
        removed_for_pid = 0

        while True:
            t0 = time.time()
            cur = conn.execute(
                """
                DELETE FROM properties_labeled
                WHERE rowid IN (
                    SELECT rowid FROM properties_labeled
                    WHERE pid = ?
                    LIMIT ?
                )
                """,
                (pid, CHUNK_SIZE),
            )
            conn.commit()

            n = cur.rowcount
            if n == 0:
                break

            removed_for_pid += n
            removed_grand_total += n
            elapsed = time.time() - t0
            left = total - removed_for_pid
            print(
                f"  – deleted {n} rows in {elapsed:.2f}s; {left} left (pid={pid}).",
                flush=True,
            )

        pid_elapsed = time.time() - pid_start
        print(f"🗑️  Finished PID={pid!r}: removed {removed_for_pid} rows in {pid_elapsed:.2f}s.")
        print("-" * 60)

    total_elapsed = time.time() - total_start
    print(f"✅ Cleanup complete: removed {removed_grand_total} rows in {total_elapsed:.2f}s.")
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Chunked-clean unwanted PIDs from wikidata_labeled.db"
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB,
        help="Path to the SQLite database file",
    )
    args = parser.parse_args()
    clean_database(args.db)
