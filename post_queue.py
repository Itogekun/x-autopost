import os
import json
import tempfile
from datetime import datetime, timezone, timedelta

import requests
from requests_oauthlib import OAuth1
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# ===== Settings =====
SHEET_NAME = os.getenv("SHEET_NAME", "queue")
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
JST = timezone(timedelta(hours=9))

HEADERS = ["scheduled_at", "text", "status", "posted_at", "tweet_id", "error_message"]

# 投稿暴走防止：1回の実行で最大何件まで投稿するか
MAX_POSTS_PER_RUN = int(os.getenv("MAX_POSTS_PER_RUN", "1"))

# ===== Env required =====
# X
X_API_KEY = os.environ["X_API_KEY"]
X_API_KEY_SECRET = os.environ["X_API_KEY_SECRET"]
X_ACCESS_TOKEN = os.environ["X_ACCESS_TOKEN"]
X_ACCESS_TOKEN_SECRET = os.environ["X_ACCESS_TOKEN_SECRET"]

# Sheets
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]

# サービスアカウントJSONは Secrets から環境変数で渡す（推奨）
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]

# ===== X client =====
auth = OAuth1(X_API_KEY, X_API_KEY_SECRET, X_ACCESS_TOKEN, X_ACCESS_TOKEN_SECRET)

def post_to_x(text: str) -> str:
    url = "https://api.x.com/2/tweets"
    res = requests.post(url, auth=auth, json={"text": text}, timeout=30)
    if res.status_code >= 300:
        raise RuntimeError(f"{res.status_code} {res.text}")
    return res.json()["data"]["id"]

# ===== Sheets helpers =====
def get_sheets_service():
    # JSON文字列を一時ファイルに落としてCredentialsへ
    sa = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(sa, f)
        sa_path = f.name

    creds = Credentials.from_service_account_file(sa_path, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)

def get_all_rows(svc):
    rng = f"{SHEET_NAME}!A1:F"
    resp = svc.spreadsheets().values().get(spreadsheetId=SPREADSHEET_ID, range=rng).execute()
    values = resp.get("values", [])
    if not values:
        return []

    if values[0] != HEADERS:
        raise RuntimeError(f"Header mismatch. expected={HEADERS}, got={values[0]}")

    rows = []
    for i, row in enumerate(values[1:], start=2):  # sheet row index
        row = (row + [""] * 6)[:6]
        rows.append({
            "row_index": i,
            "scheduled_at": row[0],
            "text": row[1],
            "status": row[2],
            "posted_at": row[3],
            "tweet_id": row[4],
            "error_message": row[5],
        })
    return rows

def update_row_full(svc, row_index: int, new_row_6cols: list[str]):
    rng = f"{SHEET_NAME}!A{row_index}:F{row_index}"
    svc.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=rng,
        valueInputOption="RAW",
        body={"values": [new_row_6cols]}
    ).execute()

def parse_scheduled_at(s: str) -> datetime:
    # "YYYY-MM-DD HH:MM"
    return datetime.strptime(s, "%Y-%m-%d %H:%M").replace(tzinfo=JST)

def run():
    svc = get_sheets_service()
    now = datetime.now(JST)

    rows = get_all_rows(svc)

    targets = []
    for r in rows:
        if not r["scheduled_at"]:
            continue
        if r["tweet_id"]:
            continue
        if r["status"] not in ("PENDING", "READY"):
            continue
        if not r["text"].strip():
            continue

        try:
            sched = parse_scheduled_at(r["scheduled_at"])
        except Exception:
            # scheduled_at不正
            new_row = [r["scheduled_at"], r["text"], "ERROR", r["posted_at"], r["tweet_id"], "invalid scheduled_at"]
            update_row_full(svc, r["row_index"], new_row)
            continue

        if sched <= now:
            targets.append((sched, r))

    targets.sort(key=lambda x: x[0])

    posted = 0
    for _, r in targets[:MAX_POSTS_PER_RUN]:
        try:
            tid = post_to_x(r["text"].strip())
            new_row = [
                r["scheduled_at"],
                r["text"],
                "POSTED",
                now.strftime("%Y-%m-%d %H:%M"),
                str(tid),
                "",
            ]
            update_row_full(svc, r["row_index"], new_row)
            print(f"POSTED row={r['row_index']} tweet_id={tid}")
            posted += 1
        except Exception as e:
            msg = str(e)[:500]
            new_row = [r["scheduled_at"], r["text"], "ERROR", r["posted_at"], r["tweet_id"], msg]
            update_row_full(svc, r["row_index"], new_row)
            print(f"ERROR row={r['row_index']} err={msg}")

    if posted == 0:
        print("No posts to send now.")

if __name__ == "__main__":
    run()
