#!/usr/bin/env python3
import argparse
import bz2
import sqlite3
import re
import os

# Updated regex for English language literals:
# This now matches literals ending with @en or @en-<variant> (e.g., @en-gb)
LANG_EN_RE = re.compile(r'"(.+)"@en(?:-[A-Za-z0-9]+)?\s*$')

# Pattern to detect URL-based values (e.g. images, external links)
URL_RE = re.compile(r'^<http[s]?://')

def create_database(db_path):
    """
    Create (if not exists) and return a connection to the SQLite database.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS properties (
            qid TEXT,
            pid TEXT,
            value TEXT,
            property_label TEXT,
            value_label TEXT
        )
    ''')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_qid ON properties(qid);')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_pid ON properties(pid);')
    conn.commit()
    return conn

def process_line(line):
    """
    Parse an N-Triple line, returning (subject, predicate, object)
    if it meets our criteria: an English literal (with tag @en or @en-variant)
    or a QID link not referencing a URL.
    Otherwise, return None.
    """
    parts = line.strip().split(" ", 3)
    if len(parts) < 4:
        return None

    subject, predicate, obj, dot = parts

    # If object is a literal (starts with a quote), check for English label/description.
    if obj.startswith('"'):
        match = LANG_EN_RE.search(obj)
        if not match:
            return None
        value_clean = match.group(1)
    else:
        # If it's a URI, skip if it references an external URL (images, websites, etc.).
        if URL_RE.search(obj):
            return None
        value_clean = obj  # Possibly another QID

    return subject, predicate, value_clean

def insert_triple(cursor, qid, pid, value, prop_label=None, value_label=None):
    """
    Insert a single triple into the 'properties' table.
    """
    cursor.execute(
        '''INSERT INTO properties (qid, pid, value, property_label, value_label)
           VALUES (?, ?, ?, ?, ?)''',
        (qid, pid, value, prop_label, value_label)
    )

def get_qid(subject):
    """
    Extract the bare QID (e.g., "Q42") from the subject URI.
    For example: "<http://www.wikidata.org/entity/Q42>" becomes "Q42".
    """
    if subject.startswith("<") and subject.endswith(">"):
        return subject[1:-1].split("/")[-1]
    return subject

def stream_parse_dump(dump_path, human_qid_set, conn):
    """
    Read the compressed dump in streaming mode, storing relevant triples
    for QIDs in human_qid_set that also have English labels/descriptions.
    """
    cursor = conn.cursor()
    # Filter keys now match substrings from the full URIs in the dump.
    filter_keys = [
        "rdf-schema#label",      # Matches <http://www.w3.org/2000/01/rdf-schema#label>
        "skos/core#prefLabel",   # Matches <http://www.w3.org/2004/02/skos/core#prefLabel>
        "schema.org/name",       # Matches <http://schema.org/name>
        "schema.org/description" # Matches <http://schema.org/description> (if present)
    ]
    
    with bz2.open(dump_path, 'rt', encoding='utf-8') as f:
        for line in f:
            triple = process_line(line)
            if not triple:
                continue

            subject, predicate, obj_value = triple

            # Extract the bare QID from the subject URI.
            qid_extracted = get_qid(subject)
            if qid_extracted not in human_qid_set:
                continue

            # Insert if the predicate contains one of the filter keys.
            if any(key in predicate for key in filter_keys):
                insert_triple(cursor, qid_extracted, predicate, obj_value)
    conn.commit()

def preload_cache(conn):
    """
    Optionally load all results into a Python dict for fast lookups.
    Key = QID, Value = list of [pid, value, property_label, value_label].
    """
    cache = {}
    cursor = conn.cursor()
    cursor.execute("SELECT qid, pid, value, property_label, value_label FROM properties")
    for row in cursor.fetchall():
        qid = row[0]
        cache.setdefault(qid, []).append(row[1:])
    return cache

def load_human_qids(qid_file):
    """
    Load QIDs (human entities) from a text file, one QID per line.
    """
    with open(qid_file, 'r', encoding='utf-8') as f:
        return {line.strip() for line in f if line.strip()}

def main():
    parser = argparse.ArgumentParser(
        description=("Test run of building an SQLite-based key-value store "
                     "for a small set of QIDs on a small dump sample.")
    )
    # Default paths for the sample data in the same scripts folder
    default_dump = os.path.join(".", "sample.nt.bz2")
    default_qids = os.path.join(".", "sample_qids.txt")
    default_db   = os.path.join(".", "test_wikidata.db")

    parser.add_argument("--dump", default=default_dump,
                        help="Path to the small sample dump file (e.g., sample.nt.bz2)")
    parser.add_argument("--qids", default=default_qids,
                        help="Path to the sample QIDs file (e.g., sample_qids.txt)")
    parser.add_argument("--db", default=default_db,
                        help="Path to the SQLite database (e.g., test_wikidata.db)")

    args = parser.parse_args()

    print("Creating and connecting to database:", args.db)
    conn = create_database(args.db)

    print("Loading list of sample QIDs from:", args.qids)
    human_qids = load_human_qids(args.qids)
    print(f"Loaded {len(human_qids)} QIDs.")

    print("Streaming and parsing N-Triples from:", args.dump)
    stream_parse_dump(args.dump, human_qids, conn)

    print("Optionally preloading the entire dataset into memory (cache).")
    cache = preload_cache(conn)
    print(f"Cache loaded. Entities in cache: {len(cache)}.")

    print("Done. Test data stored in the SQLite database and cache if needed.")

if __name__ == "__main__":
    main()
