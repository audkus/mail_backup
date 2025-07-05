import sqlite3
import time
from typing import Optional, Any

# Adjust this import to match where your function is actually implemented.
from main import update_email_address_stats

DB_FILE = "email_tracking.db"


def fetch_all_emails(conn: sqlite3.Connection):
    """
    Generator that yields email rows as dictionaries.
    """
    cur = conn.cursor()
    cur.execute('SELECT "email_pk", "from", "to", "cc", "bcc", "date", "subject", "headers" FROM email')
    cols = [desc[0] for desc in cur.description]
    for row in cur.fetchall():
        yield dict(zip(cols, row))


def batch_update(conn: sqlite3.Connection):
    """
    Update email_address stats for every email in the database.
    """
    cur = conn.cursor()
    cur.execute('SELECT COUNT(*) FROM email')
    total_emails = cur.fetchone()[0]

    print(f"Found {total_emails} emails to process.")
    processed = 0
    start_time = time.time()

    for email in fetch_all_emails(conn):
        processed += 1
        # Call your stats updater here
        update_email_address_stats(
            conn,
            sender=email.get("from"),
            recipients=(email.get("to") or []) + (email.get("cc") or []) + (email.get("bcc") or []),
            email_date=email.get("date"),
            subject=email.get("subject"),
            msg_headers=email.get("headers")
        )

        if processed % 1000 == 0 or processed == total_emails:
            elapsed = time.time() - start_time
            pct = (processed * 100) // total_emails if total_emails else 100
            print(f"Processed {processed}/{total_emails} emails ({pct}%) in {elapsed:.1f} sec")

    print("Batch update complete.")


def main():
    conn = sqlite3.connect(DB_FILE)
    try:
        batch_update(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
