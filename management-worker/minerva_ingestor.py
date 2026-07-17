#!/usr/bin/env python3
"""Minerva international datafeed ingestor.

Downloads large CSV/ZIP/GZIP feeds to /tmp, streams rows, normalizes dynamic
headers, and batch-upserts offers. Missing or invalid sources never fabricate data.
"""
from __future__ import annotations

import csv
import gzip
import hashlib
import io
import json
import logging
import os
import tempfile
import threading
import time
import zipfile
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Iterator
from urllib.parse import urlparse

import requests

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s │ %(levelname)-7s │ minerva │ %(message)s")
logger = logging.getLogger("minerva")
SUPABASE_URL = (os.getenv("SUPABASE_PROJECT_HOST_URL") or os.getenv("NEXT_PUBLIC_SUPABASE_URL") or "").rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_SECRET_KEY") or ""
AFFILIATE_ID = os.getenv("SHOPEE_AFFILIATE_ID", "")
INTERVAL_SECONDS = int(os.getenv("MINERVA_INGEST_INTERVAL_SECONDS", "86400"))
BATCH_SIZE = int(os.getenv("MINERVA_FEED_BATCH_SIZE", "250"))
MAX_ROWS = int(os.getenv("MINERVA_FEED_MAX_ROWS", "250000"))
run_lock = threading.Lock()
state = {"running": False, "started_at": None, "finished_at": None, "offers": 0, "error": None}

ALIASES = {
    "name": ["product name", "product_name", "item name", "item_name", "name", "title"],
    "url": ["affiliate link", "affiliate_link", "product link", "product_link", "offer link", "url"],
    "image": ["image url", "image_url", "image", "product image", "image link", "image_link"],
    "price": ["price", "sale price", "sale_price", "current price"],
    "category": [
        "category", "category name", "product category",
        "global category3", "global_category3",
        "global category2", "global_category2",
        "global category1", "global_category1",
    ],
    "commission": ["commission", "commission rate", "commission_rate", "commission percentage"],
    "id": ["product id", "product_id", "item id", "item_id", "itemid", "sku"],
    "shipping": [
        "shipping", "shipping info", "logistics", "freight", "tags",
        "cb option", "cb_option", "cross border", "cross_border",
    ],
}

def headers(prefer: str | None = None) -> dict[str, str]:
    result = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}", "Content-Type": "application/json"}
    if prefer:
        result["Prefer"] = prefer
    return result

def sb_get(table: str, params: dict[str, str]) -> list[dict[str, Any]]:
    response = requests.get(f"{SUPABASE_URL}/rest/v1/{table}", params=params, headers=headers(), timeout=30)
    response.raise_for_status()
    return response.json()

def sb_patch(table: str, filters: dict[str, str], payload: dict[str, Any]) -> None:
    response = requests.patch(f"{SUPABASE_URL}/rest/v1/{table}", params=filters, headers=headers("return=minimal"), json=payload, timeout=30)
    response.raise_for_status()

def normalize_header(value: str) -> str:
    return " ".join(value.strip().lower().replace("-", " ").replace("_", " ").split())

def pick(row: dict[str, Any], key: str) -> str:
    normalized = {normalize_header(str(k)): v for k, v in row.items()}
    for alias in ALIASES[key]:
        value = normalized.get(normalize_header(alias))
        if value not in (None, ""):
            return str(value).strip()
    return ""

def number(value: str) -> float:
    clean = value.replace("%", "").replace("R$", "").replace("$", "").strip()
    if clean.count(",") == 1 and clean.count(".") == 0:
        clean = clean.replace(",", ".")
    elif clean.count(",") >= 1:
        clean = clean.replace(",", "")
    try:
        return float(clean)
    except ValueError:
        return 0.0

def affiliate_url(url: str) -> str:
    if not AFFILIATE_ID or "affiliate_id=" in url:
        return url
    separator = "&" if "?" in url else "?"
    return f"{url}{separator}affiliate_id={AFFILIATE_ID}"

def download(source: dict[str, Any]) -> Path:
    parsed = urlparse(source["feed_url"])
    suffix = Path(parsed.path).suffix or ".feed"
    fd, filename = tempfile.mkstemp(prefix="minerva-", suffix=suffix, dir="/tmp")
    os.close(fd)
    path = Path(filename)
    with requests.get(source["feed_url"], stream=True, timeout=(20, 300), allow_redirects=True) as response:
        response.raise_for_status()
        with path.open("wb") as output:
            for chunk in response.iter_content(1024 * 1024):
                if chunk:
                    output.write(chunk)
    return path

def text_stream(path: Path) -> Iterator[io.TextIOBase]:
    magic = path.read_bytes()[:4]
    if magic.startswith(b"PK"):
        with zipfile.ZipFile(path) as archive:
            for name in archive.namelist():
                if name.lower().endswith((".csv", ".txt", ".tsv")):
                    with archive.open(name) as raw:
                        yield io.TextIOWrapper(raw, encoding="utf-8-sig", errors="replace", newline="")
    elif magic[:2] == b"\x1f\x8b":
        with gzip.open(path, "rt", encoding="utf-8-sig", errors="replace", newline="") as stream:
            yield stream
    else:
        with path.open("r", encoding="utf-8-sig", errors="replace", newline="") as stream:
            yield stream

def normalized_rows(source: dict[str, Any], stream: io.TextIOBase) -> Iterator[dict[str, Any]]:
    sample = stream.read(8192)
    stream.seek(0)
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
    except csv.Error:
        dialect = csv.excel
    for index, row in enumerate(csv.DictReader(stream, dialect=dialect)):
        if index >= MAX_ROWS:
            break
        name, url = pick(row, "name"), pick(row, "url")
        if not name or not url.startswith(("http://", "https://")):
            continue
        shipping = pick(row, "shipping").lower()
        if source["region"].upper() == "GLOBAL" and any(tag in shipping for tag in (
            "somente brasil", "br only", "doméstico br", "domestico br",
            "domestic br", "non-cross border", "non cross border",
        )):
            continue
        price = number(pick(row, "price"))
        commission = number(pick(row, "commission"))
        if 0 < commission <= 1:
            commission *= 100
        product_id = pick(row, "id") or hashlib.sha256(f"{source['platform']}|{url}".encode()).hexdigest()[:40]
        yield {
            "marketplace_name": source["platform"],
            "product_name": name,
            "affiliate_url": affiliate_url(url),
            "image_url": pick(row, "image") or None,
            "category": pick(row, "category") or None,
            "price": round(price, 2),
            "commission_percentage": round(commission, 2),
            "commission_value_estimated": round(price * commission / 100, 2),
            "is_automated": True,
            "original_platform_id": product_id,
            "source_feed_id": source["id"],
            "region": source["region"],
            "status": "active",
            "notes": f"Datafeed internacional {source['region']}.",
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

def upsert_batch(batch: list[dict[str, Any]]) -> None:
    response = requests.post(
        f"{SUPABASE_URL}/rest/v1/minerva_offers",
        params={"on_conflict": "marketplace_name,original_platform_id"},
        headers=headers("resolution=merge-duplicates,return=minimal"),
        json=batch,
        timeout=90,
    )
    response.raise_for_status()

def ingest_source(source: dict[str, Any]) -> int:
    path = download(source)
    processed = 0
    try:
        batch: list[dict[str, Any]] = []
        for stream in text_stream(path):
            for offer in normalized_rows(source, stream):
                batch.append(offer)
                if len(batch) >= BATCH_SIZE:
                    upsert_batch(batch)
                    processed += len(batch)
                    batch.clear()
        if batch:
            upsert_batch(batch)
            processed += len(batch)
        sb_patch("minerva_feed_sources", {"id": f"eq.{source['id']}"}, {
            "last_ingested_at": datetime.now(timezone.utc).isoformat(), "last_status": "success",
            "last_error": None, "rows_processed": processed, "updated_at": datetime.now(timezone.utc).isoformat(),
        })
        return processed
    except Exception as exc:
        sb_patch("minerva_feed_sources", {"id": f"eq.{source['id']}"}, {"last_status": "failed", "last_error": str(exc)[:1000]})
        raise
    finally:
        path.unlink(missing_ok=True)

def run_once() -> int:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("Supabase service credentials are not configured")
    sources = sb_get("minerva_feed_sources", {"active": "eq.true", "select": "*", "order": "created_at.asc"})
    total = sum(ingest_source(source) for source in sources)
    logger.info("Datafeed cycle complete sources=%s offers=%s", len(sources), total)
    return total

def background_run() -> None:
    if not run_lock.acquire(blocking=False):
        return
    state.update(running=True, started_at=datetime.now(timezone.utc).isoformat(), error=None)
    try:
        state["offers"] = run_once()
    except Exception as exc:
        state["error"] = str(exc)
        logger.exception("Datafeed cycle failed")
    finally:
        state.update(running=False, finished_at=datetime.now(timezone.utc).isoformat())
        run_lock.release()

class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        body = json.dumps({"success": True, **state}).encode()
        self.send_response(200); self.send_header("Content-Type", "application/json"); self.end_headers(); self.wfile.write(body)
    def do_POST(self) -> None:
        if self.path != "/run":
            self.send_response(404); self.end_headers(); return
        if state["running"]:
            status, payload = 202, {"success": True, "accepted": False, "reason": "already_running", **state}
        else:
            threading.Thread(target=background_run, daemon=True).start()
            status, payload = 202, {"success": True, "accepted": True}
        body = json.dumps(payload).encode()
        self.send_response(status); self.send_header("Content-Type", "application/json"); self.end_headers(); self.wfile.write(body)
    def log_message(self, fmt: str, *args: Any) -> None:
        logger.info("http " + fmt, *args)

def main() -> None:
    if "--once" in os.sys.argv:
        run_once(); return
    server = ThreadingHTTPServer(("0.0.0.0", int(os.getenv("MINERVA_INGEST_PORT", "8090"))), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    while True:
        background_run()
        time.sleep(INTERVAL_SECONDS)

if __name__ == "__main__":
    main()
