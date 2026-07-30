from __future__ import annotations

import base64
import importlib.util
import json
import os
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
REBUILD_API = ROOT / "api" / "admin" / "rebuild-booking-board.py"


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load {path}.")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


rebuild_api = load_module(REBUILD_API, "booking_board_rebuild_api_for_whatsapp_batches")
WHATSAPP_BATCH_WAIT_SECONDS = int(os.environ.get("WHATSAPP_BATCH_WAIT_SECONDS", "90") or "90")


def json_response(handler: BaseHTTPRequestHandler, status: int, payload: dict):
    body = json.dumps(payload).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def is_authorized_cron(handler: BaseHTTPRequestHandler) -> bool:
    auth_header = str(handler.headers.get("authorization") or "").strip()
    cron_secret = os.environ.get("CRON_SECRET", "").strip()
    if cron_secret and auth_header == f"Bearer {cron_secret}":
        return True
    if str(handler.headers.get("x-vercel-cron") or "").strip() == "1":
        return True
    forwarded_for = str(handler.headers.get("x-forwarded-for") or "").strip()
    return not forwarded_for or forwarded_for.startswith("127.0.0.1") or forwarded_for.startswith("::1")


def supabase_request(method: str, table_path: str, body=None, prefer: str = "return=representation") -> object:
    supabase_url = os.environ.get("SUPABASE_URL", "").strip().rstrip("/")
    service_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()
    if not supabase_url or not service_key:
        raise RuntimeError("Set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY before processing WhatsApp booking batches.")
    data = None if body is None else json.dumps(body).encode("utf-8")
    request = urllib.request.Request(
        f"{supabase_url}/rest/v1/{table_path}",
        data=data,
        method=method,
        headers={
            "apikey": service_key,
            "Authorization": f"Bearer {service_key}",
            "Content-Type": "application/json",
            "Prefer": prefer,
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=45) as response:
            raw = response.read().decode("utf-8")
            return json.loads(raw) if raw else None
    except urllib.error.HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Supabase {error.code}: {detail[:500]}") from error


def parse_timestamp(value: str) -> datetime:
    raw = str(value or "").replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return datetime.now(timezone.utc)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def fetch_queued_rows() -> list[dict]:
    query = urllib.parse.urlencode({
        "status": "eq.queued",
        "select": "id,sender,recipient,board_key,label,source_name,pdf_base64,created_at",
        "order": "created_at.asc",
        "limit": "200",
    })
    rows = supabase_request("GET", f"whatsapp_booking_board_uploads?{query}", prefer="")
    return rows if isinstance(rows, list) else []


def update_rows(row_ids: list[str], values: dict):
    if not row_ids:
        return
    quoted = ",".join(row_ids)
    supabase_request(
        "PATCH",
        f"whatsapp_booking_board_uploads?id=in.({quoted})",
        values,
        prefer="return=minimal",
    )


def group_ready_batches(rows: list[dict]) -> list[list[dict]]:
    by_sender: dict[str, list[dict]] = {}
    for row in rows:
        sender = str(row.get("sender") or "").strip()
        if sender:
            by_sender.setdefault(sender, []).append(row)
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=WHATSAPP_BATCH_WAIT_SECONDS)
    ready = []
    for sender_rows in by_sender.values():
        latest = max(parse_timestamp(row.get("created_at")) for row in sender_rows)
        if latest <= cutoff:
            ready.append(sender_rows)
    return ready


def choose_latest_uploads(rows: list[dict]) -> tuple[dict[str, bytes], list[str]]:
    latest_by_board = {}
    replaced = []
    for row in rows:
        board_key = str(row.get("board_key") or "").strip()
        if board_key not in rebuild_api.TARGETS:
            continue
        previous = latest_by_board.get(board_key)
        if previous:
            replaced.append(rebuild_api.TARGETS[board_key]["label"])
        if not previous or parse_timestamp(row.get("created_at")) >= parse_timestamp(previous.get("created_at")):
            latest_by_board[board_key] = row
    uploaded_targets = {}
    for board_key, row in latest_by_board.items():
        uploaded_targets[board_key] = base64.b64decode(str(row.get("pdf_base64") or ""))
    return uploaded_targets, sorted(set(replaced))


def twilio_auth_header() -> str:
    account_sid = os.environ.get("TWILIO_ACCOUNT_SID", "").strip()
    auth_user = os.environ.get("TWILIO_API_KEY_SID", "").strip() or account_sid
    auth_pass = os.environ.get("TWILIO_API_KEY_SECRET", "").strip() or os.environ.get("TWILIO_AUTH_TOKEN", "").strip()
    if not account_sid or not auth_user or not auth_pass:
        return ""
    return "Basic " + base64.b64encode(f"{auth_user}:{auth_pass}".encode("utf-8")).decode("ascii")


def send_whatsapp_message(to_number: str, from_number: str, body: str):
    account_sid = os.environ.get("TWILIO_ACCOUNT_SID", "").strip()
    auth_header = twilio_auth_header()
    if not account_sid or not auth_header or not to_number or not from_number:
        raise RuntimeError("Set TWILIO_ACCOUNT_SID and Twilio credentials before sending WhatsApp confirmations.")
    form = urllib.parse.urlencode({
        "To": to_number,
        "From": from_number,
        "Body": body,
    }).encode("utf-8")
    request = urllib.request.Request(
        f"https://api.twilio.com/2010-04-01/Accounts/{urllib.parse.quote(account_sid)}/Messages.json",
        data=form,
        method="POST",
        headers={
            "Authorization": auth_header,
            "Content-Type": "application/x-www-form-urlencoded",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            response.read()
    except urllib.error.HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Twilio confirmation failed with HTTP {error.code}: {detail[:500]}") from error


def process_batch(rows: list[dict]) -> dict:
    row_ids = [str(row.get("id")) for row in rows if row.get("id")]
    update_rows(row_ids, {"status": "processing"})
    sender = str(rows[0].get("sender") or "").strip()
    recipient = str(rows[0].get("recipient") or "").strip()
    try:
        uploaded_targets, replaced = choose_latest_uploads(rows)
        if not uploaded_targets:
            raise RuntimeError("No matched booking-board PDFs were queued.")
        with tempfile.TemporaryDirectory(prefix="booking-boards-whatsapp-batch-") as temp_root:
            temp_source_dir = Path(temp_root) / "Booking_Boards"
            rebuild_api.copy_sources(temp_source_dir)
            for key, pdf_bytes in uploaded_targets.items():
                (temp_source_dir / rebuild_api.TARGETS[key]["filename"]).write_bytes(pdf_bytes)
            payload = rebuild_api.build_payload(temp_source_dir)
            repo, branch, updated_at, payload = rebuild_api.commit_updates(uploaded_targets, payload, list(uploaded_targets.keys()))
        update_rows(row_ids, {"status": "processed", "processed_at": datetime.now(timezone.utc).isoformat()})
        labels = ", ".join(rebuild_api.TARGETS[key]["label"] for key in uploaded_targets)
        replaced_note = f" Latest copy used for duplicate: {', '.join(replaced)}." if replaced else ""
        message = f"Updated {labels}. {len(payload['boards'])} boards rebuilt and committed to {repo}/{branch}. Live site may take about a minute. {updated_at}.{replaced_note}"
        send_whatsapp_message(sender, recipient, message)
        return {"ok": True, "sender": sender, "rows": len(rows), "updated": list(uploaded_targets.keys())}
    except Exception as error:
        error_message = str(error)[:900]
        update_rows(row_ids, {"status": "error", "error": error_message, "processed_at": datetime.now(timezone.utc).isoformat()})
        if sender and recipient:
            try:
                send_whatsapp_message(sender, recipient, f"Booking board batch update failed: {error_message}")
            except Exception:
                pass
        return {"ok": False, "sender": sender, "rows": len(rows), "error": error_message}


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if not is_authorized_cron(self):
            json_response(self, 401, {"ok": False, "error": "Unauthorized cron request."})
            return
        try:
            rows = fetch_queued_rows()
            batches = group_ready_batches(rows)
            results = [process_batch(batch) for batch in batches]
            json_response(self, 200, {
                "ok": True,
                "queuedRows": len(rows),
                "readyBatches": len(batches),
                "waitSeconds": WHATSAPP_BATCH_WAIT_SECONDS,
                "results": results,
            })
        except Exception as error:
            json_response(self, 500, {"ok": False, "error": str(error)[:900]})
