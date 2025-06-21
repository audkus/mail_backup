import os
import json
import sqlite3
import mimetypes
from typing import List, Dict, Set, Optional
from imap_tools import MailBox, AND, MailMessage
import fitz
from docx import Document
import getpass
import keyring
from datetime import datetime
import traceback
import time

# Global list of failed emails (UID, folder)
failed_emails: List[tuple] = []

# === CONFIGURATION ===
EMAIL = "steffen@audkus.dk"
IMAP_SERVER = 'imap.one.com'
KEYCHAIN_SERVICE = "imap_email_backup"
ATTACHMENTS_DIR = 'email_attachments'
METADATA_FILE = 'email_backup.json'
DB_FILE = 'email_tracking.db'

# Try to get password from Keychain
PASSWORD = keyring.get_password(KEYCHAIN_SERVICE, EMAIL)

# If not found, prompt and store
if PASSWORD is None:
    print(f"🔐 No password found for {EMAIL}. Please enter it below.")
    PASSWORD = getpass.getpass("Enter your email password or app password: ")
    keyring.set_password(KEYCHAIN_SERVICE, EMAIL, PASSWORD)
    print("✅ Password securely stored in macOS Keychain.")


def get_max_uid(conn: sqlite3.Connection, folder: str) -> Optional[int]:
    cur = conn.cursor()
    cur.execute("SELECT MAX(CAST(uid AS INTEGER)) FROM downloaded_emails WHERE folder = ?", (folder,))
    row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else None


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
    conn.commit()
    return cur.lastrowid


def get_or_create_email(
    conn: sqlite3.Connection,
    uid: str,
    folder: str,
    subject: str,
    date: str,
    body_text: str,
    body_html: str,
    attachment_dir: str,
    message_id: str
) -> int:
    if attachment_dir is not None and not isinstance(attachment_dir, str):
        attachment_dir = str(attachment_dir)
    if message_id is not None and not isinstance(message_id, str):
        message_id = str(message_id)

    cur = conn.cursor()
    cur.execute("""
        INSERT OR IGNORE INTO email(uid, folder, subject, date, body_text, body_html, attachment_dir, message_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (uid, folder, subject, date, body_text, body_html, attachment_dir, message_id))
    conn.commit()
    cur.execute("SELECT email_pk FROM email WHERE uid = ? AND folder = ? AND message_id = ?", (uid, folder, message_id))
    return cur.fetchone()[0]


def parse_recipients(s) -> List[str]:
    """
    Accepts a string (comma/semicolon separated) or a list of addresses.
    Returns a flat list of email addresses (stripped, non-empty).
    """
    import re
    if not s:
        return []
    if isinstance(s, list):
        emails = []
        for item in s:
            emails.extend([e.strip() for e in re.split(r'[;,]', item) if e.strip()])
        return emails
    elif isinstance(s, str):
        return [e.strip() for e in re.split(r'[;,]', s) if e.strip()]
    else:
        return []


def add_participant(conn: sqlite3.Connection, email_pk: int, email_addr: str, role: str) -> None:
    email_id = get_or_create_email_address(conn, email_addr)
    if email_id:
        cur = conn.cursor()
        cur.execute(
            "INSERT OR IGNORE INTO email_participant(email_pk, email_id, role) VALUES (?, ?, ?)",
            (email_pk, email_id, role)
        )
        conn.commit()


def split_into_batches(uids: List[str], batch_size: int) -> List[List[str]]:
    return [uids[i:i + batch_size] for i in range(0, len(uids), batch_size)]


def get_existing_uids(conn: sqlite3.Connection, folder: str) -> Set[str]:
    cur = conn.cursor()
    cur.execute("SELECT uid FROM downloaded_emails WHERE folder = ?", (folder,))
    return {row[0] for row in cur.fetchall()}


def log_missing_uids(conn: sqlite3.Connection, folder: str, all_uids: List[str]) -> None:
    """Log UIDs from IMAP that are missing from the database."""
    cur = conn.cursor()
    cur.execute("SELECT uid FROM downloaded_emails WHERE folder = ?", (folder,))
    existing_uids = set(row[0] for row in cur.fetchall())

    missing_uids = [uid for uid in all_uids if uid not in existing_uids]
    if missing_uids:
        log(f"⚠️ {len(missing_uids)} missing UIDs in folder {folder}")
        filename = f"missing_uids_{folder.replace('/', '_').replace(' ', '_')}.log"
        with open(filename, 'w', encoding='utf-8') as f:
            for uid in missing_uids:
                f.write(f"{uid}\n")
        log(f"📄 Missing UIDs logged to {filename}")
    else:
        log(f"✅ All UIDs in {folder} are already downloaded.")


def log_error(uid: str, folder: str, error: Exception) -> None:
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open("error.log", "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] UID: {uid} | Folder: {folder}\n")
        f.write(f"Error: {str(error)}\n")
        f.write(traceback.format_exc())
        f.write("\n" + "=" * 60 + "\n")
    failed_emails.append((uid, folder))


def get_uid_range(mailbox: MailBox, folder: str) -> tuple[int, int]:
    mailbox.folder.set(folder)
    uids = [int(msg.uid) for msg in mailbox.fetch(AND(all=True), mark_seen=False)]
    return (min(uids), max(uids)) if uids else (0, 0)


def fetch_uids_in_batches(mailbox: MailBox, folder: str, batch_size: int = 500) -> List[List[int]]:
    mailbox.folder.set(folder)
    uids = sorted([int(msg.uid) for msg in mailbox.fetch(AND(all=True), mark_seen=False)])
    return [uids[i:i + batch_size] for i in range(0, len(uids), batch_size)]


def process_folder(mailbox, folder: str, saved_emails: list, conn):
    total_count = 0
    downloaded_count = 0
    try:
        mailbox.folder.set(folder)
        for msg in mailbox.fetch(AND(all=True), reverse=True):
            total_count += 1
            uid = msg.uid
            if is_email_downloaded(conn, uid, folder):
                log(f"⏭️  Skipping already downloaded UID {uid} in folder {folder}")
                continue
            try:
                email_date = msg.date.strftime('%Y-%m-%d %H:%M:%S')
                log(f"⬇️ Downloading email UID {uid} from {folder} [{email_date}]: {msg.subject}")
                email_data = extract_email_data(msg, uid, folder)
                saved_emails.append(email_data)
                mark_email_downloaded(
                    conn,
                    uid,
                    folder,
                    msg.subject,
                    msg.from_,
                    ", ".join(msg.to or []),
                    ", ".join(msg.cc or []),
                    ", ".join(msg.bcc or []),
                    msg.date.isoformat(),
                    msg.text,
                    msg.html,
                    email_data['attachments'][0] if email_data['attachments'] else None,
                    email_data['message_id']
                )

                downloaded_count += 1

                # Real-time normalization logic
                email_pk = get_or_create_email(
                    conn,
                    uid,
                    folder,
                    msg.subject,
                    msg.date.isoformat(),
                    msg.text,
                    msg.html,
                    email_data['attachments'][0] if email_data['attachments'] else None,
                    email_data['message_id']
                )
                # Sender (should always be a single address string)
                add_participant(conn, email_pk, msg.from_, 'from')

                # Recipients (to, cc, bcc)
                for to_addr in parse_recipients(msg.to):
                    add_participant(conn, email_pk, to_addr, 'to')
                for cc_addr in parse_recipients(msg.cc):
                    add_participant(conn, email_pk, cc_addr, 'cc')
                for bcc_addr in parse_recipients(msg.bcc):
                    add_participant(conn, email_pk, bcc_addr, 'bcc')

            except Exception as e:
                log(f"❌ Error processing UID {uid}: {e}")
                log_error(uid, folder, e)

        log(f"📨 Finished folder: {folder}")
        log(f"   🔹 Total fetched: {total_count}")
        log(f"   ✅ New downloaded: {downloaded_count}")
        log(f"   ⏭️ Already in DB: {total_count - downloaded_count}")

    except Exception as e:
        log(f"❌ Folder fetch failed for {folder}: {e}")
        raise


def ensure_dirs() -> None:
    os.makedirs(ATTACHMENTS_DIR, exist_ok=True)


def init_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE)
    conn.execute('''
        CREATE TABLE IF NOT EXISTS downloaded_emails (
            uid TEXT,
            folder TEXT,
            subject TEXT,
            sender TEXT,
            recipients TEXT,
            cc TEXT,
            bcc TEXT,
            date TEXT,
            body_text TEXT,
            body_html TEXT,
            attachment_dir TEXT,
            message_id TEXT,
            PRIMARY KEY (uid, folder)
        )
    ''')

    conn.execute('''
        CREATE TABLE IF NOT EXISTS categories (
            category_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            description TEXT,
            parent_id INTEGER,
            FOREIGN KEY (parent_id) REFERENCES categories(category_id)
        )
    ''')

    conn.execute('''
        CREATE TABLE IF NOT EXISTS email_categories (
            uid TEXT NOT NULL,
            folder TEXT NOT NULL,
            category_id INTEGER NOT NULL,
            PRIMARY KEY (uid, folder, category_id),
            FOREIGN KEY (category_id) REFERENCES categories(category_id)
        )
    ''')

    conn.execute('''
        CREATE TABLE IF NOT EXISTS folder_uidvalidity (
            folder TEXT PRIMARY KEY,
            uidvalidity INTEGER NOT NULL
        )
    ''')

    conn.execute("CREATE INDEX IF NOT EXISTS idx_downloaded_emails_message_id ON downloaded_emails(message_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_downloaded_emails_folder ON downloaded_emails(folder)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_email_categories_category_id ON email_categories(category_id)")

    create_normalized_tables(conn)

    return conn


def get_uidvalidity(conn: sqlite3.Connection, folder: str) -> Optional[int]:
    cur = conn.cursor()
    cur.execute('SELECT uidvalidity FROM folder_uidvalidity WHERE folder = ?', (folder,))
    row = cur.fetchone()
    return row[0] if row else None


def set_uidvalidity(conn: sqlite3.Connection, folder: str, uidvalidity: int) -> None:
    cur = conn.cursor()
    cur.execute('''
        INSERT OR REPLACE INTO folder_uidvalidity (folder, uidvalidity) VALUES (?, ?)
    ''', (folder, uidvalidity))
    conn.commit()


def is_email_downloaded(conn: sqlite3.Connection, uid: str, folder: str) -> bool:
    cur = conn.cursor()
    cur.execute('SELECT 1 FROM downloaded_emails WHERE uid = ? AND folder = ?', (uid, folder))
    return cur.fetchone() is not None


def mark_email_downloaded(
    conn: sqlite3.Connection,
    uid: str,
    folder: str,
    subject: str,
    sender: str,
    recipients: str,
    cc: str,
    bcc: str,
    date: str,
    body_text: str,
    body_html: str,
    attachment_dir: str,
    message_id: str
) -> None:
    if attachment_dir is not None and not isinstance(attachment_dir, str):
        attachment_dir = str(attachment_dir)
    if message_id is not None and not isinstance(message_id, str):
        message_id = str(message_id)
    conn.execute(
        '''INSERT OR IGNORE INTO downloaded_emails
           (uid, folder, subject, sender, recipients, cc, bcc, date, body_text, body_html, attachment_dir, message_id)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
        (uid, folder, subject, sender, recipients, cc, bcc, date, body_text, body_html, attachment_dir, message_id)
    )
    conn.commit()


def extract_text_from_attachment(file_path: str) -> str:
    """Extract text from supported attachment types."""
    ext = os.path.splitext(file_path)[1].lower()
    try:
        if ext == '.pdf':
            return extract_text_from_pdf(file_path)
        elif ext == '.docx':
            return extract_text_from_docx(file_path)
        elif ext == '.txt':
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                return f.read()
        else:
            return f"[Unsupported attachment type: {ext}]"
    except Exception as e:
        return f"[Error extracting {file_path}: {e}]"


def extract_text_from_pdf(file_path: str) -> str:
    text = []
    with fitz.open(file_path) as doc:
        for page in doc:
            text.append(page.get_text())
    return '\n'.join(text)


def extract_text_from_docx(file_path: str) -> str:
    doc = Document(file_path)
    return '\n'.join(p.text for p in doc.paragraphs)


def extract_email_data(msg: MailMessage, uid: str, folder: str) -> Dict:
    safe_folder = folder.replace('/', '_').replace(' ', '_')

    attachment_paths = []
    attachment_texts = []

    if msg.attachments:
        email_year = msg.date.year
        email_month = f"{msg.date.month:02}"
        email_dir = os.path.join(ATTACHMENTS_DIR, safe_folder, str(email_year), email_month, uid)
        os.makedirs(email_dir, exist_ok=True)

        for i, att in enumerate(msg.attachments):
            mime_type = att.content_type  # e.g. 'application/pdf'
            extension = mimetypes.guess_extension(mime_type) or '.bin'
            filename = att.filename or f'attachment_{i}{extension}'
            filename = filename.strip().replace('/', '_').replace('\\', '_')
            att_path = os.path.join(email_dir, filename)
            if os.path.isdir(att_path):
                att_path += "_file"
            with open(att_path, 'wb') as f:
                f.write(att.payload)
            attachment_paths.append(att_path)
            extracted_text = extract_text_from_attachment(att_path)
            attachment_texts.append({'filename': filename, 'text': extracted_text})

    # --- Always extract message_id regardless of attachments ---
    message_id = None
    if hasattr(msg, 'headers'):
        for k in msg.headers.keys():
            if k.lower() == "message-id":
                value = msg.headers[k]
                if value is not None:
                    message_id = str(value)
                break

    return {
        'uid': uid,
        'folder': folder,
        'subject': msg.subject,
        'from': msg.from_,
        'to': msg.to,
        'cc': msg.cc,
        'bcc': msg.bcc,
        'date': msg.date.isoformat(),
        'text': msg.text,
        'html': msg.html,
        'attachments': attachment_paths,
        'attachment_text': attachment_texts,
        'message_id': message_id
    }


# def save_metadata(all_emails: List[Dict]) -> None:
#     with open(METADATA_FILE, 'w', encoding='utf-8') as f:
#         json.dump(all_emails, f, indent=2, ensure_ascii=False)


def log(message: str) -> None:
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] {message}")


def backfill_from_json(conn: sqlite3.Connection) -> None:
    if not os.path.exists(METADATA_FILE):
        log("ℹ️ No metadata file found for backfill.")
        return

    with open(METADATA_FILE, 'r', encoding='utf-8') as f:
        data = json.load(f)

    inserted = 0
    for item in data:
        uid = item['uid']
        folder = item['folder']
        if not is_email_downloaded(conn, uid, folder):
            mark_email_downloaded(
                conn,
                uid,
                folder,
                item.get('subject', ''),
                item.get('from', ''),
                ", ".join(item.get('to') or []),
                ", ".join(item.get('cc') or []),
                ", ".join(item.get('bcc') or []),
                item.get('date', ''),
                item.get('text', ''),
                item.get('html', ''),
                item['attachments'][0] if item.get('attachments') else None,
                item.get('message_id')
            )
            inserted += 1

    log(f"📥 Backfill complete: {inserted} records added from JSON.")


def reset_logs() -> None:
    """Truncate log files at the start of execution."""
    for log_file in ["error.log", "failed_folders.log"]:
        with open(log_file, "w", encoding="utf-8"):
            pass  # This clears the file


def create_normalized_tables(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
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
            attachment_dir TEXT,
            message_id TEXT
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
    cur.execute("CREATE INDEX IF NOT EXISTS idx_email_message_id ON email(message_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_email_participant_email_id ON email_participant(email_id)")

    conn.commit()


def ensure_folder_and_uidvalidity(mailbox: MailBox, conn: sqlite3.Connection, folder: str) -> None:
    mailbox.folder.set(folder)
    # Get all status fields (no argument)
    status = mailbox.folder.status()
    log(f"DEBUG: Folder status for '{folder}': {status}")
    # Defensive: handle both cases (just in case some server returns lowercase keys)
    uidvalidity_key = None
    for key in status:
        if key.upper() == "UIDVALIDITY":
            uidvalidity_key = key
            break
    if not uidvalidity_key:
        raise RuntimeError(f"Folder status for '{folder}' does not contain UIDVALIDITY. Returned: {status}")
    current_uidvalidity = int(status[uidvalidity_key])
    stored_uidvalidity = get_uidvalidity(conn, folder)
    if stored_uidvalidity is not None and stored_uidvalidity != current_uidvalidity:
        log(f"❌ UIDVALIDITY mismatch for folder '{folder}'! Stored: {stored_uidvalidity}, Current: {current_uidvalidity}. Aborting backup.")
        raise RuntimeError(f"UIDVALIDITY mismatch for folder '{folder}'! Backup halted.")
    elif stored_uidvalidity is None:
        set_uidvalidity(conn, folder, current_uidvalidity)


def main() -> None:
    reset_logs()
    ensure_dirs()
    conn = init_db()
    saved_emails = []
    failed_folders = []

    if os.path.exists(METADATA_FILE):
        with open(METADATA_FILE, 'r', encoding='utf-8') as f:
            saved_emails = json.load(f)

    try:
        with MailBox(IMAP_SERVER).login(EMAIL, PASSWORD) as mailbox:
            folders_to_process = [f.name for f in mailbox.folder.list()]
            log(f"📂 Found folders: {folders_to_process}")

            for folder in folders_to_process:
                skip_folders = {'[gmail]/spam', '[gmail]/trash', 'inbox.spam', 'inbox.trash', 'junk', 'trash'}
                if folder.lower() in skip_folders:
                    log(f"🚫 Skipping folder: {folder}")
                    continue

                retry_limit = 3
                attempt = 0

                try:
                    ensure_folder_and_uidvalidity(mailbox, conn, folder)
                except RuntimeError as e:
                    log(str(e))
                    break

                while attempt < retry_limit:
                    attempt += 1
                    try:
                        log(f"🔄 Processing folder: {folder} (attempt {attempt})")
                        mailbox.folder.set(folder)

                        max_uid = get_max_uid(conn, folder)
                        if max_uid is not None:
                            all_uids = [msg.uid for msg in mailbox.fetch(AND(uid=f"{max_uid + 1}:*"), mark_seen=False)]
                            log(f"📬 {folder}: Only fetching UIDs > {max_uid}")
                        else:
                            all_uids = [msg.uid for msg in mailbox.fetch(AND(all=True), mark_seen=False)]
                            log(f"📬 {folder}: First run, fetching all UIDs")

                        existing_uids = get_existing_uids(conn, folder)
                        log(f"📬 {folder} contains {len(all_uids)} messages")
                        log_missing_uids(conn, folder, all_uids)

                        uid_batches = split_into_batches(all_uids, batch_size=10)

                        for batch in uid_batches:
                            if all(uid in existing_uids for uid in batch):
                                log(f"⏭️  Skipping batch: all {len(batch)} UIDs already downloaded.")
                                continue

                            messages = list(mailbox.fetch(AND(uid=batch), mark_seen=False))
                            for msg in messages:
                                uid = msg.uid
                                if uid in existing_uids:
                                    continue
                                try:
                                    email_date = msg.date.strftime('%Y-%m-%d %H:%M:%S')
                                    log(f"⬇️  Downloading email UID {uid} from {folder} [{email_date}]: {msg.subject}")
                                    email_data = extract_email_data(msg, uid, folder)
                                    saved_emails.append(email_data)
                                    mark_email_downloaded(
                                        conn, uid, folder, msg.subject, msg.from_,
                                        ", ".join(msg.to or []),
                                        ", ".join(msg.cc or []),
                                        ", ".join(msg.bcc or []),
                                        msg.date.isoformat(), msg.text, msg.html,
                                        email_data['attachments'][0] if email_data['attachments'] else None,
                                        email_data['message_id']
                                    )
                                    email_pk = get_or_create_email(
                                        conn, uid, folder, msg.subject, msg.date.isoformat(), msg.text, msg.html,
                                        email_data['attachments'][0] if email_data['attachments'] else None,
                                        email_data['message_id']
                                    )
                                    add_participant(conn, email_pk, msg.from_, 'from')
                                    for to_addr in parse_recipients(msg.to):
                                        add_participant(conn, email_pk, to_addr, 'to')
                                    for cc_addr in parse_recipients(msg.cc):
                                        add_participant(conn, email_pk, cc_addr, 'cc')
                                    for bcc_addr in parse_recipients(msg.bcc):
                                        add_participant(conn, email_pk, bcc_addr, 'bcc')
                                except Exception as e:
                                    log(f"❌ Error processing UID {uid}: {e}")
                                    log_error(uid, folder, e)
                        break  # Success
                    except Exception as e:
                        log(f"⚠️ Folder {folder} failed on attempt {attempt}: {e}")
                        if attempt == retry_limit:
                            log(f"❌ Folder {folder} failed after {retry_limit} attempts.")
                            failed_folders.append(folder)
                        else:
                            time.sleep(5)
    except Exception as e:
        log(f"❌ Initial connection failed: {e}")
        return

    # Retry failed emails
    if failed_emails:
        log(f"🔁 Retrying {len(failed_emails)} failed emails...")
        for uid, folder in failed_emails:
            try:
                with MailBox(IMAP_SERVER).login(EMAIL, PASSWORD) as mailbox:
                    mailbox.folder.set(folder)
                    msg = next(mailbox.fetch(AND(uid=uid), mark_seen=False))
                    log(f"🔄 Retrying UID {uid} from {folder}: {msg.subject}")
                    email_data = extract_email_data(msg, uid, folder)
                    saved_emails.append(email_data)
                    mark_email_downloaded(
                        conn, uid, folder, msg.subject, msg.from_,
                        ", ".join(msg.to or []), ", ".join(msg.cc or []), ", ".join(msg.bcc or []),
                        msg.date.isoformat(), msg.text, msg.html,
                        email_data['attachments'][0] if email_data['attachments'] else None,
                        email_data['message_id']
                    )
                    log(f"✅ Retry successful for UID {uid}")
            except Exception as e:
                log(f"❌ Retry failed again for UID {uid} in folder {folder}: {e}")
                log_error(uid, folder, e)

    if failed_folders:
        with open("failed_folders.log", "w", encoding="utf-8") as f:
            f.write("Folders that failed after all retry attempts:\n")
            for folder in failed_folders:
                f.write(f"{folder}\n")
        log(f"🛑 {len(failed_folders)} folders failed. See 'failed_folders.log' for details.")

    log("✅ Backup completed and saved.")


if __name__ == '__main__':
    main()
