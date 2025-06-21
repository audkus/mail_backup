import sqlite3
import re
import time
from typing import Optional, List, Tuple

DB_FILE = 'email_tracking.db'
BATCH_SIZE = 5000  # Commit every 5000 emails (not recipients)
PRINT_FREQ = 1000  # Print progress every 1000 emails


def safe_commit(conn, retries=10, wait=2):
    for i in range(retries):
        try:
            conn.commit()
            return
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e).lower():
                print(f"DB is locked, retrying ({i+1}/{retries})...")
                time.sleep(wait)
            else:
                raise
    raise RuntimeError("Could not commit after several retries (database is locked)")


def create_tables(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    # Main normalized tables
    cur.execute('''
        CREATE TABLE IF NOT EXISTS person (
            person_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            company TEXT,
            organization TEXT,
            phone TEXT
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS email_address (
            email_id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE NOT NULL
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS person_email (
            person_id INTEGER,
            email_id INTEGER,
            PRIMARY KEY (person_id, email_id)
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS email (
            email_pk INTEGER PRIMARY KEY AUTOINCREMENT,
            uid TEXT,
            folder TEXT,
            subject TEXT,
            date TEXT,
            body_text TEXT,
            body_html TEXT,
            attachment_dir TEXT
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS email_participant (
            email_pk INTEGER,
            email_id INTEGER,
            role TEXT, -- 'from', 'to', 'cc', 'bcc'
            PRIMARY KEY (email_pk, email_id, role)
        )
    ''')
    safe_commit(conn)


def get_or_create_email_address(conn: sqlite3.Connection, email: str) -> Optional[int]:
    if not email:
        return None
    email = email.strip().lower()
    cur = conn.cursor()
    cur.execute("SELECT email_id FROM email_address WHERE email = ?", (email,))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("INSERT INTO email_address(email) VALUES (?)", (email,))
    safe_commit(conn)
    return cur.lastrowid


def get_or_create_email(conn: sqlite3.Connection, row: sqlite3.Row) -> int:
    cur = conn.cursor()
    cur.execute("""
        INSERT OR IGNORE INTO email(uid, folder, subject, date, body_text, body_html, attachment_dir)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
    row["uid"], row["folder"], row["subject"], row["date"], row["body_text"], row["body_html"], row["attachment_dir"]))
    safe_commit(conn)
    cur.execute("SELECT email_pk FROM email WHERE uid = ? AND folder = ?", (row["uid"], row["folder"]))
    return cur.fetchone()[0]


def parse_recipients(s: Optional[str]) -> List[str]:
    if not s:
        return []
    return [e.strip().lower() for e in re.split(r'[;,]', s) if e.strip()]


# def main():
#     conn = sqlite3.connect(DB_FILE)
#     conn.row_factory = sqlite3.Row
#     create_tables(conn)
#     cur = conn.cursor()
#     cur.execute("SELECT * FROM downloaded_emails")
#     rows = cur.fetchall()
#     processed = 0
#
#     for row in rows:
#         email_pk = get_or_create_email(conn, row)
#
#         # Sender (role: 'from')
#         sender_email = row["sender"]
#         sender_email_id = get_or_create_email_address(conn, sender_email)
#         if sender_email_id:
#             cur.execute(
#                 "INSERT OR IGNORE INTO email_participant(email_pk, email_id, role) VALUES (?, ?, ?)",
#                 (email_pk, sender_email_id, 'from')
#             )
#
#         # Recipients (role: 'to')
#         to_emails = parse_recipients(row["recipients"])
#         for rec_email in to_emails:
#             rec_email_id = get_or_create_email_address(conn, rec_email)
#             if rec_email_id:
#                 cur.execute(
#                     "INSERT OR IGNORE INTO email_participant(email_pk, email_id, role) VALUES (?, ?, ?)",
#                     (email_pk, rec_email_id, 'to')
#                 )
#
#         # CC
#         cc_emails = parse_recipients(row["cc"])
#         for cc_email in cc_emails:
#             cc_email_id = get_or_create_email_address(conn, cc_email)
#             if cc_email_id:
#                 cur.execute(
#                     "INSERT OR IGNORE INTO email_participant(email_pk, email_id, role) VALUES (?, ?, ?)",
#                     (email_pk, cc_email_id, 'cc')
#                 )
#
#         # BCC
#         bcc_emails = parse_recipients(row["bcc"])
#         for bcc_email in bcc_emails:
#             bcc_email_id = get_or_create_email_address(conn, bcc_email)
#             if bcc_email_id:
#                 cur.execute(
#                     "INSERT OR IGNORE INTO email_participant(email_pk, email_id, role) VALUES (?, ?, ?)",
#                     (email_pk, bcc_email_id, 'bcc')
#                 )
#             processed += 1
#             if processed % BATCH_SIZE == 0:
#                 safe_commit(conn)
#                 print(f"Committed {processed} rows...")
#
#         # Optionally: print progress for huge DBs
#         # print(f"Processed email {row['uid']} in {row['folder']}")
#
#     safe_commit(conn)
#     print("✅ Migration to normalized tables complete.")


def main():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    create_tables(conn)
    cur = conn.cursor()
    cur.execute("SELECT * FROM downloaded_emails")
    rows = cur.fetchall()
    total = len(rows)
    print(f"Total emails to process: {total}")
    processed = 0
    start_time = time.time()

    for row in rows:
        email_pk = get_or_create_email(conn, row)
        # Sender
        sender_email = row["sender"]
        sender_email_id = get_or_create_email_address(conn, sender_email)
        if sender_email_id:
            cur.execute(
                "INSERT OR IGNORE INTO email_participant(email_pk, email_id, role) VALUES (?, ?, ?)",
                (email_pk, sender_email_id, 'from')
            )
        # Recipients
        for rec_email in parse_recipients(row["recipients"]):
            rec_email_id = get_or_create_email_address(conn, rec_email)
            if rec_email_id:
                cur.execute(
                    "INSERT OR IGNORE INTO email_participant(email_pk, email_id, role) VALUES (?, ?, ?)",
                    (email_pk, rec_email_id, 'to')
                )
        # CC
        for cc_email in parse_recipients(row["cc"]):
            cc_email_id = get_or_create_email_address(conn, cc_email)
            if cc_email_id:
                cur.execute(
                    "INSERT OR IGNORE INTO email_participant(email_pk, email_id, role) VALUES (?, ?, ?)",
                    (email_pk, cc_email_id, 'cc')
                )
        # BCC
        for bcc_email in parse_recipients(row["bcc"]):
            bcc_email_id = get_or_create_email_address(conn, bcc_email)
            if bcc_email_id:
                cur.execute(
                    "INSERT OR IGNORE INTO email_participant(email_pk, email_id, role) VALUES (?, ?, ?)",
                    (email_pk, bcc_email_id, 'bcc')
                )

        processed += 1
        if processed % BATCH_SIZE == 0 or processed == total:
            safe_commit(conn)
            elapsed = time.time() - start_time
            pct = (processed / total) * 100
            eta = (elapsed / processed) * (total - processed) if processed > 0 else 0
            msg = f"Processed {processed:,}/{total:,} ({pct:.1f}%) | Elapsed: {elapsed/60:.1f} min | ETA: {eta/60:.1f} min"
            # Print progress on one line (overwrites)
            print('\r' + msg, end='', flush=True)
    # Final commit (if anything left)
    safe_commit(conn)
    print("\n✅ Migration to normalized tables complete.")


if __name__ == "__main__":
    main()

# Processed 135,000/154,155 (87.6%) | Elapsed: 8654.8 min | ETA: 1228.0 min
# Processed 140,000/154,155 (90.8%) | Elapsed: 12102.1 min | ETA: 1223.6 min
# Processed 145,000/154,155 (94.1%) | Elapsed: 13781.9 min | ETA: 870.2 min
# Processed 150,000/154,155 (97.3%) | Elapsed: 16030.5 min | ETA: 444.0 min