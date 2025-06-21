#!/usr/bin/env python3
"""
Export the `downloaded_emails` table to JSON files
split by year-month (YYYY-MM) and capped at ~29.5 MB each.

File names:  YYYY-MM-001.json, YYYY-MM-002.json, …
Output dir : ./json_exports
"""

import os
import json
import sqlite3
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any

# ── CONFIG ────────────────────────────────────────────────────────────────────
DB_PATH          = "email_tracking.db"   # change if your DB is elsewhere
TABLE_NAME       = "downloaded_emails"   # change if you used a different table
OUTPUT_DIR       = "json_exports"
MAX_FILE_MB      = 29.5                  # un-compressed file size limit
MAX_FILE_BYTES   = int(MAX_FILE_MB * 1024 * 1024)
# ──────────────────────────────────────────────────────────────────────────────


def row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
    """Convert sqlite3.Row → regular dict and post-process list fields."""
    d = dict(row)
    # Split comma-separated recipient fields into lists
    for fld in ("recipients", "cc", "bcc"):
        raw = d.get(fld)
        d[fld] = [x.strip() for x in raw.split(",")] if raw else []
    return d


def main() -> None:
    Path(OUTPUT_DIR).mkdir(exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # stream rows without loading entire table at once
    cur.execute(f"SELECT * FROM {TABLE_NAME}")

    # per-month buffers: { 'YYYY-MM': [email,…] }
    buffers: Dict[str, List[Dict[str, Any]]] = {}
    sizes:   Dict[str, int] = {}  # current bytes per month buffer
    file_counters: Dict[str, int] = {}  # next part number per month

    def flush(month: str) -> None:
        """Write current buffer for month to disk if any."""
        if month not in buffers or not buffers[month]:
            return
        part = file_counters.get(month, 1)
        fn = f"{month}-{part:03}.json"
        path = Path(OUTPUT_DIR) / fn
        with path.open("w", encoding="utf-8") as f:
            json.dump(buffers[month], f, indent=2, ensure_ascii=False)
        print(f"✅ wrote {fn} ({len(buffers[month])} emails)")
        file_counters[month] = part + 1
        buffers[month].clear()
        sizes[month] = 0

    for row in cur:  # streaming rows
        email = row_to_dict(row)
        try:
            month_key = datetime.fromisoformat(email["date"]).strftime("%Y-%m")
        except Exception:
            month_key = "unknown"

        if month_key not in buffers:
            buffers[month_key] = []
            sizes[month_key] = 0
            file_counters[month_key] = 1

        # rough size = bytes of newly encoded email
        email_json = json.dumps(email, ensure_ascii=False)
        est_size = len(email_json.encode("utf-8"))

        # flush if adding this email would exceed limit
        if sizes[month_key] + est_size > MAX_FILE_BYTES and buffers[month_key]:
            flush(month_key)

        buffers[month_key].append(email)
        sizes[month_key] += est_size

    # Flush any remaining buffers
    for m in list(buffers):
        flush(m)

    conn.close()
    print("🏁 export complete.")


if __name__ == "__main__":
    main()
