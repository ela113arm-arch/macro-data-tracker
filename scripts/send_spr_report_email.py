"""Email the latest generated SPR release report from GitHub Actions.

This is optional. The workflow only runs it when SMTP credentials and a
recipient are configured as GitHub secrets/variables.
"""

from __future__ import annotations

import os
import smtplib
from email.message import EmailMessage
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REPORT_DIR = ROOT / "reports" / "spr"
LATEST_MD = REPORT_DIR / "spr_release_report_latest.md"
LATEST_HTML = REPORT_DIR / "spr_release_report_latest.html"


def main() -> int:
    username = os.environ["SMTP_USERNAME"]
    password = os.environ["SMTP_PASSWORD"]
    recipient = os.environ["SPR_REPORT_EMAIL_TO"]

    if not LATEST_MD.exists():
        raise FileNotFoundError(f"Missing report file: {LATEST_MD}")

    msg = EmailMessage()
    msg["Subject"] = "Monthly SPR release tracker"
    msg["From"] = username
    msg["To"] = recipient
    msg.set_content(LATEST_MD.read_text(encoding="utf-8"))

    if LATEST_HTML.exists():
        msg.add_alternative(LATEST_HTML.read_text(encoding="utf-8"), subtype="html")

    with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=30) as smtp:
        smtp.login(username, password)
        smtp.send_message(msg)

    print(f"Sent SPR report email to {recipient}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
