#!/usr/bin/env python3
"""Create a list of QIDs with clean birth dates.

Reads ``birthdays.json`` (a mapping of QID to birth date strings) and
writes ``qid_with_bd_clean.txt`` containing only the QIDs whose birth
date value is exactly ``YYYY-MM-DD``.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

# scripts/ -> Repo/ -> Text-Embeddings/ -> WikiData.nosync/
BASE = Path(__file__).resolve().parent.parent.parent / "WikiData.nosync"
DEFAULT_IN = BASE / "birthdays.json"
DEFAULT_OUT = BASE / "qid_with_bd_clean.txt"

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def load_birthdays(path: Path) -> dict[str, str]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def main(in_path: Path, out_path: Path) -> None:
    data = load_birthdays(in_path)
    qids = [qid for qid, val in data.items() if DATE_RE.fullmatch(val)]
    with open(out_path, "w", encoding="utf-8") as f:
        for qid in qids:
            f.write(f"{qid}\n")
    print(f"\u2705 Wrote {len(qids)} clean birthdays to {out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Filter birthdays.json for dates formatted YYYY-MM-DD",
    )
    parser.add_argument("--in", dest="input_path", type=Path, default=DEFAULT_IN,
                        help="Path to birthdays.json")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT,
                        help="Output text file")
    args = parser.parse_args()
    main(args.input_path, args.out)

