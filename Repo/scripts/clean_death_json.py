#!/usr/bin/env python3
"""Filter death_dates.json and output death_dates_clean.json with valid dates."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

# scripts/ -> Repo/ -> Text-Embeddings/ -> WikiData.nosync/
BASE = Path(__file__).resolve().parent.parent.parent / "WikiData.nosync"
DEFAULT_IN = BASE / "death_dates.json"
DEFAULT_OUT = BASE / "death_dates_clean.json"

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def load_birthdays(path: Path) -> dict[str, str]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def main(in_path: Path, out_path: Path) -> None:
    data = load_birthdays(in_path)
    clean = {qid: date for qid, date in data.items() if DATE_RE.fullmatch(date)}
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(clean, f, ensure_ascii=False, indent=2)
    print(f"\u2705 Wrote {len(clean)} clean death dates to {out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create death_dates_clean.json with dates formatted YYYY-MM-DD",
    )
    parser.add_argument("--in", dest="input_path", type=Path, default=DEFAULT_IN,
                        help="Path to death_dates.json")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT,
                        help="Output JSON file")
    args = parser.parse_args()
    main(args.input_path, args.out)
