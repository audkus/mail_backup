import os
import sqlite3
import mimetypes
import json
import time
from typing import List, Dict
from imap_tools import MailBox, AND, MailMessage
from datetime import datetime
import fitz
from docx import Document
import keyring
import getpass

EMAIL = "steffen@audkus.dk"
IMAP_SERVER = 'imap.one.com'
KEYCHAIN_SERVICE = "imap_email_backup"
ATTACHMENTS_DIR = 'email_attachments'
DB_FILE = 'email_tracking.db'
MISSING_UIDS_LOG = 'missing_uids_INBOX.log'
METADATA_FILE = 'email_backup.json'
LOG_FILE = 'recovery.log'


def log(message: str) -> None:
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    full_message = f"[{timestamp}] {message}"
    print(full_message)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(full_message + "\n")


def ensure_dirs() -> None:
    os.makedirs(ATTACHMENTS_DIR, exist_ok=True)


def init_db() -> sqlite3.Connection:
    return sqlite3.connect(DB_FILE)


def is_email_downloaded(conn: sqlite3.Connection, uid: str, folder: str) -> bool:
    cur = conn.cursor()
    cur.execute('SELECT 1 FROM downloaded_emails WHERE uid = ? AND folder = ?', (uid, folder))
    return cur.fetchone() is not None


def mark_email_downloaded(conn: sqlite3.Connection, uid: str, folder: str, subject: str, sender: str,
                          recipients: str, cc: str, bcc: str, date: str, body_text: str,
                          body_html: str, attachment_dir: str) -> None:
    conn.execute('''
        INSERT OR IGNORE INTO downloaded_emails
        (uid, folder, subject, sender, recipients, cc, bcc, date, body_text, body_html, attachment_dir)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
        (uid, folder, subject, sender, recipients, cc, bcc, date, body_text, body_html, attachment_dir))
    conn.commit()


def extract_text_from_attachment(file_path: str) -> str:
    ext = os.path.splitext(file_path)[1].lower()
    try:
        if ext == '.pdf':
            with fitz.open(file_path) as doc:
                return '\n'.join(page.get_text() for page in doc)
        elif ext == '.docx':
            return '\n'.join(p.text for p in Document(file_path).paragraphs)
        elif ext == '.txt':
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                return f.read()
        return f"[Unsupported attachment type: {ext}]"
    except Exception as e:
        return f"[Error extracting {file_path}: {e}]"


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
            mime_type = att.content_type
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
        'attachment_text': attachment_texts
    }


def load_missing_uids(path: str) -> List[str]:
    with open(path, 'r', encoding='utf-8') as f:
        return [line.strip() for line in f if line.strip().isdigit()]


def split_into_batches(uids: List[str], batch_size: int) -> List[List[str]]:
    return [uids[i:i + batch_size] for i in range(0, len(uids), batch_size)]


def main():
    ensure_dirs()
    conn = init_db()

    password = keyring.get_password(KEYCHAIN_SERVICE, EMAIL)
    if password is None:
        print("No password found in keychain. Please enter it:")
        password = getpass.getpass("Password: ")
        keyring.set_password(KEYCHAIN_SERVICE, EMAIL, password)

    all_missing_uids = load_missing_uids(MISSING_UIDS_LOG)
    cur = conn.cursor()
    cur.execute("SELECT uid FROM downloaded_emails WHERE folder = 'INBOX'")
    already_downloaded = {row[0] for row in cur.fetchall()}
    to_fetch = [uid for uid in all_missing_uids if uid not in already_downloaded]
    log(f"🔍 Found {len(to_fetch)} UIDs to recover.")

    if not to_fetch:
        log("✅ Nothing to do.")
        return

    recovered = []
    batches = split_into_batches(to_fetch, batch_size=20)

    for batch in batches:
        retry_count = 0
        while retry_count < 3:
            try:
                with MailBox(IMAP_SERVER).login(EMAIL, password) as mailbox:
                    mailbox.folder.set("INBOX")
                    log(f"⬇️ Fetching batch {batch[0]} - {batch[-1]}")
                    for msg in mailbox.fetch(AND(uid=batch), mark_seen=False):
                        try:
                            uid = msg.uid
                            email_data = extract_email_data(msg, uid, "INBOX")
                            recovered.append(email_data)
                            mark_email_downloaded(
                                conn,
                                uid,
                                "INBOX",
                                msg.subject,
                                msg.from_,
                                ", ".join(msg.to or []),
                                ", ".join(msg.cc or []),
                                ", ".join(msg.bcc or []),
                                msg.date.isoformat(),
                                msg.text,
                                msg.html,
                                email_data['attachments'][0] if email_data['attachments'] else None
                            )
                        except Exception as e:
                            log(f"❌ Failed to recover email in batch {batch[0]}-{batch[-1]}: {e}")
                    break
            except Exception as e:
                retry_count += 1
                log(f"⚠️ Batch {batch[0]} - {batch[-1]} failed (attempt {retry_count}): {e}")
                time.sleep(5)
        else:
            log(f"❌ Giving up on batch {batch[0]} - {batch[-1]} after 3 attempts.")

    if recovered:
        if os.path.exists(METADATA_FILE):
            with open(METADATA_FILE, 'r', encoding='utf-8') as f:
                existing = json.load(f)
        else:
            existing = []
        existing.extend(recovered)
        with open(METADATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(existing, f, indent=2, ensure_ascii=False)
        log(f"✅ Recovery complete: {len(recovered)} messages added.")


if __name__ == '__main__':
    main()
