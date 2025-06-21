import os
import sqlite3
from imap_tools import MailBox, AND
from datetime import datetime
import keyring
import getpass
from collections import defaultdict

EMAIL = "steffen@audkus.dk"
IMAP_SERVER = "imap.one.com"
KEYCHAIN_SERVICE = "imap_email_backup"
DB_FILE = "email_tracking.db"
FOLDER = "INBOX"


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")


def get_password() -> str:
    password = keyring.get_password(KEYCHAIN_SERVICE, EMAIL)
    if not password:
        password = getpass.getpass("Enter your email password: ")
        keyring.set_password(KEYCHAIN_SERVICE, EMAIL, password)
    return password


def get_downloaded_uids(db_path: str, folder: str) -> set[str]:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT uid FROM downloaded_emails WHERE folder = ?", (folder,))
    uids = {row[0] for row in cur.fetchall()}
    conn.close()
    return uids


def categorize_by_year(mailbox: MailBox, uids: set[str]) -> dict[int, list[str]]:
    year_map = defaultdict(list)
    for msg in mailbox.fetch(AND(uid=list(uids)), mark_seen=False):
        if msg.date:
            year = msg.date.year
        else:
            year = 1900
        year_map[year].append(msg.uid)
    return year_map


def main() -> None:
    password = get_password()
    log("📡 Connecting to mail server...")
    with MailBox(IMAP_SERVER).login(EMAIL, password) as mailbox:
        mailbox.folder.set(FOLDER)
        log(f"📥 Fetching all UIDs from {FOLDER}...")
        server_uids = set(mailbox.uids())
        log(f"🔢 Total UIDs on server: {len(server_uids)}")

        downloaded_uids = get_downloaded_uids(DB_FILE, FOLDER)
        log(f"💾 Total UIDs in database: {len(downloaded_uids)}")

        missing_uids = server_uids - downloaded_uids
        log(f"❗ Missing UIDs: {len(missing_uids)}")

        log("🗃️ Categorizing missing UIDs by year...")
        year_map = categorize_by_year(mailbox, missing_uids)

        print("\n📆 Missing email counts by year:")
        total = 0
        for year in sorted(year_map.keys()):
            count = len(year_map[year])
            print(f"  {year}: {count}")
            total += count
        print(f"\n🧮 Total missing emails: {total}")


if __name__ == "__main__":
    main()
