import sqlite3
import keyring
from imap_tools import MailBox, AND
import time

DB_FILE = "email_tracking.db"
EMAIL = "steffen@audkus.dk"
IMAP_SERVER = "imap.one.com"
KEYCHAIN_SERVICE = "imap_email_backup"
PASSWORD = keyring.get_password(KEYCHAIN_SERVICE, EMAIL)


def update_message_ids():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Find emails missing message_id
    cur.execute("SELECT uid, folder FROM downloaded_emails WHERE message_id IS NULL OR message_id = ''")
    missing = cur.fetchall()
    print(f"Updating {len(missing)} emails missing message_id...")

    with MailBox(IMAP_SERVER).login(EMAIL, PASSWORD) as mailbox:
        folders_done = set()
        for idx, row in enumerate(missing, 1):
            uid, folder = row["uid"], row["folder"]
            try:
                if folder not in folders_done:
                    mailbox.folder.set(folder)
                    folders_done.add(folder)
                # Find message with matching UID
                msg = next(mailbox.fetch(AND(uid=uid), mark_seen=False))
                message_id = None
                for k in msg.headers.keys():
                    if k.lower() == "message-id":
                        message_id = msg.headers[k]
                        break
                if message_id is not None:
                    message_id = str(message_id)
                else:
                    message_id = ""
                cur.execute(
                    "UPDATE downloaded_emails SET message_id = ? WHERE uid = ? AND folder = ?",
                    (message_id, uid, folder)
                )
                cur.execute(
                    "UPDATE email SET message_id = ? WHERE uid = ? AND folder = ?",
                    (message_id, uid, folder)
                )
                if idx % 1000 == 0:
                    print(f"  ...updated {idx} of {len(missing)}")
                    conn.commit()  # commit in batches
            except Exception as e:
                print(f"Error updating UID {uid} in {folder}: {e}")

        conn.commit()
    conn.close()
    print("Message-ID update complete.")


if __name__ == "__main__":
    update_message_ids()
