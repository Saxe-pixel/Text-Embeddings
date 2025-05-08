#!/usr/bin/env python3
import json
import sqlite3
from pathlib import Path

# ---- CONFIGURE THESE BASED ON YOUR REPO STRUCTURE ----
# scripts/ → Repo/ → Text-Embeddings/ → WikiData.nosync/
BASE = Path(__file__).resolve().parent.parent.parent / "WikiData.nosync"
DB_PATH   = BASE / "wikidata.db-kopi"
JSON_PATH = BASE / "label_map_full.json"
# ------------------------------------------------------

def main():
    # 1) Load JSON of ID→label
    with open(JSON_PATH, "r", encoding="utf-8") as f:
        labels = json.load(f)

    # 2) Open SQLite
    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()

    # 3) Apply each label to properties and values
    for ident, lab in labels.items():
        # a) property_label: whenever pid contains that ID
        cur.execute("""
            UPDATE properties
               SET property_label = ?
             WHERE pid LIKE '%' || ? || '%'
        """, (lab, ident))

        # b) value_label: only when value is an entity URI
        cur.execute(f"""
            UPDATE properties
               SET value_label = ?
             WHERE value LIKE '%/entity/{ident}>'
        """, (lab,))

    conn.commit()
    conn.close()
    print(f"✅ Applied {len(labels)} labels into {DB_PATH}")

if __name__ == "__main__":
    main()
