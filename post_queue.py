#!/usr/bin/env python3
"""
post_queue.py
- Google Sheets (queue) をキューとして、予定時刻になった行を X に自動投稿する
- 画像付き投稿対応（media_path がある場合のみ 1枚アップロード）
- 7列固定（A..G）:
  scheduled_at | text | media_path | status | posted_at | tweet_id | error_message
"""

from __future__ import annotations

import os
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple

import requests
from requests_oauthlib import OAuth1
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# ===== Settings =====
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
JST = timezone(timedelta(hours=9))

HEADERS: List[str] = [
    "scheduled_at",
    "text",
    "media_path",
    "status",
    "posted_at",
    "tweet_id",
    "error_message",
]

TWEET_URL = "https://api.x.com/2/tweets"
MEDIA_UPLOAD_URL = "https://upload.twitter.com/1.1/media/upload.json"

MAX_TWEET_LEN = 280  # 日本語でも280（念のためガード）


def now_jst_str() -> str:
    return datetime.now(JST).strftime("%Y-%m-%d %H:%M")


def parse_scheduled_at(s: str) -> datetime:
    """scheduled_at: 'YYYY-MM-DD HH:MM' (JST前提)"""
    return datetime.strptime(s.strip(), "%Y-%m-%d %H:%M").replace(tzinfo=JST)


def get_sheets_service(service_account_file: str, service_account_json: Optional[str] = None):
    """
    優先順位:
      1) service_account_file が存在 -> それを使う
      2) envの JSON文字列 (GOOGLE_SERVICE_ACCOUNT_JSON) -> それを使う
    """
    p = Path(service_account_file)
    if p.exists():
        creds = Credentials.from_service_account_file(str(p), scopes=SCOPES)
        return build("sheets", "v4", credentials=creds)

    if service_account_json:
        data = json.loads(service_account_json)
        creds = Credentials.from_service_account_info(data, scopes=SCOPES)
        return build("sheets", "v4", credentials=creds)

    raise RuntimeError(
        "Service account credentials not found. "
        "Put service-account.json in workspace OR set GOOGLE_SERVICE_ACCOUNT_JSON."
    )


def ensure_header(svc, spreadsheet_id: str, sheet_name: str):
    rng = f"{sheet_name}!A1:G1"
    resp = svc.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=rng).execute()
    values = resp.get("values", [])
    if not values or values[0] != HEADERS:
        raise RuntimeError(
            "Header mismatch.\n"
            f"Expected: {HEADERS}\n"
            f"Got: {values[0] if values else None}"
        )


def read_queue_rows(svc, spreadsheet_id: str, sheet_name: str) -> List[Dict[str, Any]]:
    rng = f"{sheet_name}!A1:G"
    resp = svc.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=rng).execute()
    values = resp.get("values", [])
    if not values:
        return []

    header = values[0]
    if header != HEADERS:
        raise RuntimeError(f"Header mismatch: expected {HEADERS}, got {header}")

    rows: List[Dict[str, Any]] = []
    for i, row in enumerate(values[1:], start=2):
        row = row + [""] * (len(HEADERS) - len(row))
        rows.append(
            {
                "row_index": i,
                "scheduled_at": row[0],
                "text": row[1],
                "media_path": row[2],
                "status": row[3],
                "posted_at": row[4],
                "tweet_id": row[5],
                "error_message": row[6],
            }
        )
    return rows


def update_row_full(
    svc,
    spreadsheet_id: str,
    sheet_name: str,
    row_index: int,
    new_row_7cols: List[str],
):
    if len(new_row_7cols) != 7:
        raise ValueError("new_row_7cols must have 7 columns (A..G)")
    rng = f"{sheet_name}!A{row_index}:G{row_index}"
    svc.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=rng,
        valueInputOption="RAW",
        body={"values": [new_row_7cols]},
    ).execute()


def upload_media(auth: OAuth1, media_path: str) -> str:
    """X v1.1 media upload -> media_id_string"""
    p = Path(media_path)
    if not p.is_file():
        raise FileNotFoundError(f"media_path not found: {media_path}")

    with p.open("rb") as f:
        files = {"media": f}
        res = requests.post(MEDIA_UPLOAD_URL, auth=auth, files=files, timeout=60)

    if res.status_code >= 400:
        raise RuntimeError(f"media upload failed: {res.status_code} {res.text}")

    js = res.json()
    media_id = js.get("media_id_string") or str(js.get("media_id"))
    if not media_id:
        raise RuntimeError(f"media upload ok but media_id missing: {js}")
    return media_id


def post_to_x(auth: OAuth1, text: str, media_id: Optional[str] = None) -> str:
    payload: Dict[str, Any] = {"text": text}
    if media_id:
        payload["media"] = {"media_ids": [media_id]}

    res = requests.post(TWEET_URL, auth=auth, json=payload, timeout=30)
    if res.status_code >= 400:
        raise RuntimeError(f"tweet post failed: {res.status_code} {res.text}")

    js = res.json()
    tid = js.get("data", {}).get("id")
    if not tid:
        raise RuntimeError(f"tweet posted but id missing: {js}")
    return tid


def run():
    enable_run = os.getenv("ENABLE_RUN", "true").lower() in ("1", "true", "yes", "y", "on")
    if not enable_run:
        print("ENABLE_RUN=false -> exit without posting")
        return

    required = [
        "SPREADSHEET_ID",
        "X_API_KEY",
        "X_API_KEY_SECRET",
        "X_ACCESS_TOKEN",
        "X_ACCESS_TOKEN_SECRET",
    ]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Missing env vars: {missing}")

    spreadsheet_id = os.environ["SPREADSHEET_ID"]
    sheet_name = os.getenv("SHEET_NAME", "queue")
    service_account_file = os.getenv("SERVICE_ACCOUNT_FILE", "service-account.json")
    service_account_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")  # optional

    max_posts_per_run = int(os.getenv("MAX_POSTS_PER_RUN", "1"))

    auth = OAuth1(
        os.environ["X_API_KEY"],
        os.environ["X_API_KEY_SECRET"],
        os.environ["X_ACCESS_TOKEN"],
        os.environ["X_ACCESS_TOKEN_SECRET"],
    )

    svc = get_sheets_service(service_account_file, service_account_json)
    ensure_header(svc, spreadsheet_id, sheet_name)

    rows = read_queue_rows(svc, spreadsheet_id, sheet_name)
    now = datetime.now(JST)

    candidates: List[Tuple[datetime, Dict[str, Any]]] = []
    for r in rows:
        if not str(r["scheduled_at"]).strip():
            continue
        if str(r["tweet_id"]).strip():
            continue
        if r["status"] not in ("PENDING", "READY"):
            continue
        if not str(r["text"]).strip():
            continue

        try:
            sched = parse_scheduled_at(str(r["scheduled_at"]))
        except Exception:
            new_row = [
                str(r["scheduled_at"]),
                str(r["text"]),
                str(r["media_path"]),
                "ERROR",
                str(r["posted_at"]),
                str(r["tweet_id"]),
                "invalid scheduled_at",
            ]
            update_row_full(svc, spreadsheet_id, sheet_name, r["row_index"], new_row)
            continue

        if sched <= now:
            candidates.append((sched, r))

    candidates.sort(key=lambda x: x[0])
    candidates = candidates[:max_posts_per_run]

    posted = 0
    for _, r in candidates:
        try:
            text = str(r["text"])
            if len(text) > MAX_TWEET_LEN:
                raise ValueError(f"text too long: {len(text)} (limit {MAX_TWEET_LEN})")

            media_id: Optional[str] = None
            media_path = str(r["media_path"]).strip()
            if media_path:
                media_id = upload_media(auth, media_path)

            tid = post_to_x(auth, text, media_id=media_id)

            new_row = [
                str(r["scheduled_at"]),
                str(r["text"]),
                str(r["media_path"]),
                "POSTED",
                now_jst_str(),
                tid,
                "",
            ]
            update_row_full(svc, spreadsheet_id, sheet_name, r["row_index"], new_row)
            print(f"POSTED row={r['row_index']} tweet_id={tid}")
            posted += 1

        except Exception as e:
            msg = str(e)[:500]
            new_row = [
                str(r["scheduled_at"]),
                str(r["text"]),
                str(r["media_path"]),
                "ERROR",
                str(r["posted_at"]),
                str(r["tweet_id"]),
                msg,
            ]
            update_row_full(svc, spreadsheet_id, sheet_name, r["row_index"], new_row)
            print(f"ERROR row={r['row_index']} err={msg}")

    if posted == 0:
        print("No posts to send now.")


if __name__ == "__main__":
    # .env を使うなら（ローカル用）:
    #   pip install python-dotenv
    #   そして以下2行を有効化
    # from dotenv import load_dotenv
    # load_dotenv(override=True)
    run()
