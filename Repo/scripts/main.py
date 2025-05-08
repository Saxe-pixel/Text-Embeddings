#!/usr/bin/env python3
import argparse
import bz2
import sqlite3
import re
import os
from tqdm import tqdm  # progress bar

# Regex for English language literals (must end with "@en" before the period)
LANG_EN_RE = re.compile(r'"(.+)"@en\s*\.$')
# Pattern to detect external URL-based values (skipping only non-Wikidata entities)
URL_RE = re.compile(r'^<https?://(?!www\.wikidata\.org/entity/)')


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


def insert_triple(cursor, qid, pid, value, prop_label=None, value_label=None):
    """
    Insert a single triple into the 'properties' table.
    """
    cursor.execute(
        '''INSERT INTO properties (qid, pid, value, property_label, value_label)
           VALUES (?, ?, ?, ?, ?)''',
        (qid, pid, value, prop_label, value_label)
    )


def process_line(line):
    """
    Parse an N-Triple line, returning (subject, predicate, object)
    if it’s either an English literal (@en) or a QID URI. Otherwise return None.
    """
    line = line.strip()
    if not line.endswith('.'):
        return None
    # remove the final ' .'
    core = line[:-1].rstrip()

    # split into exactly three parts: subject, predicate, object
    parts = core.split(' ', 2)
    if len(parts) != 3:
        return None
    subject, predicate, obj = parts

    # Literal case
    if obj.startswith('"'):
        # re-append the dot for regex matching
        m = LANG_EN_RE.search(obj + '.')
        if not m:
            return None
        value_clean = m.group(1)
    else:
        # URI: skip external URLs not from Wikidata
        if URL_RE.search(obj):
            return None
        value_clean = obj

    return subject, predicate, value_clean


def get_qid(subject):
    """
    Extract the bare QID (e.g., "Q42") from the subject URI.
    """
    if subject.startswith('<') and subject.endswith('>'):
        return subject[1:-1].split('/')[-1]
    return subject


def stream_parse_dump(dump_path, human_qid_set, conn):
    """
    Read the compressed dump in streaming mode, storing relevant triples
    for QIDs in human_qid_set that also have English literals or QID links.
    A progress bar is shown.
    """
    cursor = conn.cursor()
    total = os.path.getsize(dump_path)
    with bz2.open(dump_path, 'rt', encoding='utf-8') as f, \
         tqdm(total=total, unit='B', unit_scale=True, desc='Processing') as pbar:
        for line in f:
            triple = process_line(line)
            if not triple:
                pbar.update(len(line.encode('utf-8')))
                continue

            subject, predicate, obj_value = triple
            qid_extracted = get_qid(subject)
            if qid_extracted not in human_qid_set:
                pbar.update(len(line.encode('utf-8')))
                continue

            # Insert every English/QID triple for matching QIDs
            insert_triple(cursor, qid_extracted, predicate, obj_value)
            pbar.update(len(line.encode('utf-8')))

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
        cache.setdefault(row[0], []).append(row[1:])
    return cache


def load_human_qids(qid_file):
    """
    Load QIDs (human entities) from a text file, one QID per line.
    """
    with open(qid_file, 'r', encoding='utf-8') as f:
        return {line.strip() for line in f if line.strip()}


def main():
    parser = argparse.ArgumentParser(
        description=("Build an SQLite-based key-value store from "
                     "Wikidata's truthy dump for human QIDs.")
    )
    default_dump = os.path.join('..', '..', 'WikiData.nosync', 'latest-truthy.nt.bz2')
    default_qids = os.path.join('..', '..', 'WikiData.nosync', 'human_qids.txt')
    default_db   = os.path.join('..', '..', 'WikiData.nosync', 'wikidata.db')

    parser.add_argument('--dump', default=default_dump,
                        help='Path to the latest-truthy.nt.bz2 file')
    parser.add_argument('--qids', default=default_qids,
                        help='Path to the human_qids.txt file')
    parser.add_argument('--db', default=default_db,
                        help='Output path for the SQLite database')

    args = parser.parse_args()

    print('Creating and connecting to database:', args.db)
    conn = create_database(args.db)

    print('Loading list of human QIDs from:', args.qids)
    human_qids = load_human_qids(args.qids)
    print(f'Loaded {len(human_qids)} QIDs.')

    print('Streaming and parsing N-Triples from:', args.dump)
    stream_parse_dump(args.dump, human_qids, conn)

    print('Optionally preloading the entire dataset into memory (cache).')
    cache = preload_cache(conn)
    print(f'Cache loaded. Entities in cache: {len(cache)}.')

    print('Done. The data is now stored in your SQLite database and in cache if needed.')

if __name__ == '__main__':
    main()
