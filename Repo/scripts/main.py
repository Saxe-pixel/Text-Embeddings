#!/usr/bin/env python3
import argparse
import bz2
import sqlite3
import re
import os
from tqdm import tqdm  # progress bar

# Regexes for object types
LANG_EN_RE    = re.compile(r'^"(.+)"@en$')  # plain English literal
DATETIME_RE   = re.compile(
    r'^"([^"]+)"\^\^<http://www\.w3\.org/2001/XMLSchema#dateTime>$'
)
# Skip external URLs except Wikidata entity links:
URL_RE        = re.compile(r'^<https?://(?!www\.wikidata\.org/entity/)')


def create_database(db_path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS properties (
            qid TEXT,
            pid TEXT,
            value TEXT,
            property_label TEXT,
            value_label TEXT
        )
    ''')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_qid ON properties(qid);')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_pid ON properties(pid);')
    conn.commit()
    return conn


def insert_triple(cur, qid, pid, value):
    cur.execute(
        'INSERT INTO properties (qid,pid,value,property_label,value_label) VALUES (?,?,?,?,?)',
        (qid, pid, value, None, None)
    )


def process_line(line):
    line = line.strip()
    if not line.endswith('.'):
        return None
    core = line[:-1].rstrip()
    parts = core.split(' ', 2)
    if len(parts) != 3:
        return None
    subj, pred, obj = parts

    # literal?
    if obj.startswith('"'):
        if DATETIME_RE.match(obj):
            # dateTime literal
            value_clean = DATETIME_RE.match(obj).group(1)
        else:
            m = LANG_EN_RE.match(obj)
            if not m:
                return None
            value_clean = m.group(1)
    else:
        # URI case: skip true external URLs
        if URL_RE.match(obj):
            return None
        value_clean = obj

    return subj, pred, value_clean


def get_qid(uri):
    if uri.startswith('<') and uri.endswith('>'):
        return uri[1:-1].split('/')[-1]
    return uri


def get_pid(uri):
    if uri.startswith('<') and uri.endswith('>'):
        return uri[1:-1].split('/')[-1]
    return uri


def stream_parse_dump(dump_path, human_qids, conn):
    cur = conn.cursor()
    total = os.path.getsize(dump_path)
    with bz2.open(dump_path, 'rt', encoding='utf-8') as f, \
         tqdm(total=total, unit='B', unit_scale=True, desc='Processing') as pbar:
        for line in f:
            res = process_line(line)
            pbar.update(len(line.encode('utf-8')))
            if not res:
                continue

            subj, pred, val = res
            qid = get_qid(subj)
            if qid not in human_qids:
                continue

            pid = get_pid(pred)
            # if object was a QID URI, strip to bare QID
            if val.startswith('<') and 'wikidata.org/entity/' in val:
                val = get_qid(val)

            insert_triple(cur, qid, pid, val)

    conn.commit()


def load_human_qids(path):
    with open(path, 'r', encoding='utf-8') as f:
        return {l.strip() for l in f if l.strip()}


def preload_cache(conn):
    cache = {}
    cur = conn.cursor()
    cur.execute("SELECT qid,pid,value,property_label,value_label FROM properties")
    for qid, pid, val, pl, vl in cur.fetchall():
        cache.setdefault(qid, []).append((pid, val, pl, vl))
    return cache


def main():
    p = argparse.ArgumentParser(
        description="Build SQLite store of all English/dateTime/QID triples for human QIDs"
    )
    base = os.path.join('..','..','WikiData.nosync')
    p.add_argument('--dump', default=os.path.join(base,'latest-truthy.nt.bz2'))
    p.add_argument('--qids', default=os.path.join(base,'human_qids.txt'))
    p.add_argument('--db',   default=os.path.join(base,'wikidata.db'))
    args = p.parse_args()

    print("DB:", args.db)
    conn = create_database(args.db)

    print("Loading human QIDs from", args.qids)
    humans = load_human_qids(args.qids)
    print(f"{len(humans):,} QIDs loaded")

    print("Streaming dump…")
    stream_parse_dump(args.dump, humans, conn)

    print("Preloading cache…")
    cache = preload_cache(conn)
    print(f"{len(cache):,} entities in cache")

    print("Done.")

if __name__=='__main__':
    main()
