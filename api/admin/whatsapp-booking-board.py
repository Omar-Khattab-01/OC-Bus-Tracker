from __future__ import annotations

import base64
import hashlib
import hmac
import importlib.util
import json
import os
import re
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
REBUILD_API = ROOT / "api" / "admin" / "rebuild-booking-board.py"
BUILD_SCRIPT = ROOT / "tools" / "build_booking_boards.py"
WHATSAPP_BATCH_WAIT_SECONDS = int(os.environ.get("WHATSAPP_BATCH_WAIT_SECONDS", "90") or "90")


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load {path}.")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


rebuild_api = load_module(REBUILD_API, "booking_board_rebuild_api")
builder = load_module(BUILD_SCRIPT, "booking_board_builder_for_whatsapp")


def xml_escape(value: str) -> str:
    return (
        str(value or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&apos;")
    )


def twiml_response(handler: BaseHTTPRequestHandler, message: str, status: int = 200):
    body = f'<?xml version="1.0" encoding="UTF-8"?><Response><Message>{xml_escape(message)}</Message></Response>'.encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "text/xml; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def normalize_text(value: str) -> str:
    return " ".join(str(value or "").replace("_", " ").replace("-", " ").lower().split())


def normalize_whatsapp_address(value: str) -> str:
    return "".join(str(value or "").strip().lower().split())


def parse_board_hints(text: str) -> list[str]:
    normalized = normalize_text(text)
    matches = []
    patterns = [
        ("days_off_counter", r"\b(days?\s+off|counter)\b"),
        ("vacation_tracker", r"\bvacation\b"),
        ("spares", r"\bspares?\b"),
        ("stat", r"\b(stat|canada\s+day|civic|holiday)\b"),
        ("weekend", r"\b(weekend|saturday|sunday)\b"),
        ("daily", r"\b(daily|weekday|weekdays)\b"),
    ]
    for key, pattern in patterns:
        for match in re.finditer(pattern, normalized):
            matches.append((match.start(), key))
    ordered = []
    seen = set()
    for _, key in sorted(matches):
        if key not in seen:
            ordered.append(key)
            seen.add(key)
    return ordered


def classify_pdf_text(pdf_bytes: bytes) -> str:
    with tempfile.NamedTemporaryFile(suffix=".pdf") as handle:
        handle.write(pdf_bytes)
        handle.flush()
        pages = builder.extract_lines(Path(handle.name))
    first_page = normalize_text(" ".join(pages[0].get("lines", [])[:80] if pages else []))
    text = normalize_text(" ".join(" ".join(page.get("lines", [])[:80]) for page in pages[:3]))
    if "vacation tracker" in text:
        return "vacation_tracker"
    if "fall 2026 days off" in text or "day total booked remaining" in first_page:
        return "days_off_counter"
    if "general booking spare progress report" in first_page or "floating spare" in first_page:
        return "spares"
    if "daily open work" in first_page:
        return "daily"
    if "mixed odd work saturday" in first_page or "mixed odd work sunday" in text or "sat1 sat2" in first_page:
        return "weekend"
    if "labour day" in first_page or "stat work" in text or "canada day" in text or "civic" in text or "holiday" in text:
        return "stat"
    return ""


def configured_webhook_url(handler: BaseHTTPRequestHandler) -> str:
    explicit = os.environ.get("WHATSAPP_PUBLIC_WEBHOOK_URL", "").strip()
    if explicit:
        return explicit
    proto = str(handler.headers.get("x-forwarded-proto") or "https").split(",")[0].strip()
    host = str(handler.headers.get("x-forwarded-host") or handler.headers.get("host") or "").split(",")[0].strip()
    return f"{proto}://{host}{handler.path}"


def validate_twilio_signature(handler: BaseHTTPRequestHandler, params: dict[str, str]) -> bool:
    auth_token = os.environ.get("TWILIO_AUTH_TOKEN", "").strip()
    if not auth_token:
        return True
    provided = str(handler.headers.get("x-twilio-signature") or "").strip()
    if not provided:
        return False
    payload = configured_webhook_url(handler)
    for key in sorted(params):
        payload += key + str(params[key])
    expected = base64.b64encode(hmac.new(auth_token.encode("utf-8"), payload.encode("utf-8"), hashlib.sha1).digest()).decode("ascii")
    return hmac.compare_digest(provided, expected)


def is_authorized(handler: BaseHTTPRequestHandler, params: dict[str, str]) -> bool:
    expected = os.environ.get("WHATSAPP_BOOKING_BOARD_TOKEN", "").strip() or os.environ.get("BOOKING_BOARD_ADMIN_TOKEN", "").strip()
    parsed = urllib.parse.urlparse(handler.path)
    query = urllib.parse.parse_qs(parsed.query)
    provided = str(query.get("token", [""])[0] or handler.headers.get("x-whatsapp-booking-token") or "").strip()
    if not expected or not hmac.compare_digest(expected, provided):
        return False
    if not validate_twilio_signature(handler, params):
        return False
    allowed = [normalize_whatsapp_address(item) for item in os.environ.get("WHATSAPP_ALLOWED_FROM", "").split(",") if item.strip()]
    if allowed and normalize_whatsapp_address(params.get("From", "")) not in allowed:
        return False
    return True


def filename_from_content_disposition(value: str) -> str:
    match = None
    for piece in str(value or "").split(";"):
        if "filename" in piece.lower() and "=" in piece:
            match = piece.split("=", 1)[1].strip().strip('"')
            break
    return urllib.parse.unquote(match) if match else ""


def download_media(url: str) -> tuple[bytes, str]:
    headers = {}
    media_user = os.environ.get("TWILIO_API_KEY_SID", "").strip() or os.environ.get("TWILIO_ACCOUNT_SID", "").strip()
    media_pass = os.environ.get("TWILIO_API_KEY_SECRET", "").strip() or os.environ.get("TWILIO_AUTH_TOKEN", "").strip()
    if media_user and media_pass:
        encoded = base64.b64encode(f"{media_user}:{media_pass}".encode("utf-8")).decode("ascii")
        headers["Authorization"] = f"Basic {encoded}"
    request = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=45) as response:
            return response.read(), filename_from_content_disposition(response.headers.get("content-disposition", ""))
    except urllib.error.HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Media download failed with HTTP {error.code}: {detail[:300]}") from error


def supabase_request(method: str, table_path: str, body=None) -> object:
    supabase_url = os.environ.get("SUPABASE_URL", "").strip().rstrip("/")
    service_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()
    if not supabase_url or not service_key:
        raise RuntimeError("Set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY before using WhatsApp booking batches.")
    data = None if body is None else json.dumps(body).encode("utf-8")
    request = urllib.request.Request(
        f"{supabase_url}/rest/v1/{table_path}",
        data=data,
        method=method,
        headers={
            "apikey": service_key,
            "Authorization": f"Bearer {service_key}",
            "Content-Type": "application/json",
            "Prefer": "return=representation",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            raw = response.read().decode("utf-8")
            return json.loads(raw) if raw else None
    except urllib.error.HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Supabase {error.code}: {detail[:500]}") from error


def queue_uploads(sender: str, recipient: str, matched: list[dict], uploaded_targets: dict[str, bytes]) -> int:
    rows = []
    by_board = {item["board"]: item for item in matched}
    for board_key, pdf_bytes in uploaded_targets.items():
        item = by_board.get(board_key) or {}
        rows.append({
            "sender": sender,
            "recipient": recipient,
            "board_key": board_key,
            "label": item.get("label") or rebuild_api.TARGETS[board_key]["label"],
            "source_name": item.get("sourceName") or rebuild_api.TARGETS[board_key]["filename"],
            "pdf_base64": base64.b64encode(pdf_bytes).decode("ascii"),
            "status": "queued",
        })
    if not rows:
        return 0
    inserted = supabase_request("POST", "whatsapp_booking_board_uploads", rows)
    return len(inserted or rows)


def media_items(params: dict[str, str]) -> list[dict]:
    try:
        count = int(str(params.get("NumMedia") or "0"))
    except ValueError:
        count = 0
    items = []
    for index in range(max(0, count)):
        url = str(params.get(f"MediaUrl{index}") or "").strip()
        if url:
            items.append({
                "index": index,
                "url": url,
                "content_type": str(params.get(f"MediaContentType{index}") or "").lower(),
            })
    return items


class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        try:
            length = int(str(self.headers.get("content-length") or "0"))
        except ValueError:
            length = 0
        raw_body = self.rfile.read(length)
        parsed_form = urllib.parse.parse_qs(raw_body.decode("utf-8", errors="replace"), keep_blank_values=True)
        params = {key: values[-1] if values else "" for key, values in parsed_form.items()}

        if not is_authorized(self, params):
            twiml_response(self, "Booking board WhatsApp upload is not authorized.")
            return

        items = media_items(params)
        if not items:
            twiml_response(self, "Send or forward one or more Fall booking board PDF files.")
            return

        try:
            body_text = str(params.get("Body") or "").strip()
            body_hints = parse_board_hints(body_text)
            uploaded_targets: dict[str, bytes] = {}
            matched = []
            unmatched = []

            for item in items:
                pdf_bytes, downloaded_name = download_media(item["url"])
                source_name = downloaded_name or body_text or f"whatsapp-media-{item['index'] + 1}.pdf"
                if not pdf_bytes.startswith(b"%PDF-"):
                    unmatched.append(f"{source_name} was not a PDF.")
                    continue
                board_key = (
                    rebuild_api.classify_upload_name(source_name)
                    or (body_hints[item["index"]] if len(body_hints) == len(items) else "")
                    or (body_hints[0] if len(items) == 1 and len(body_hints) == 1 else "")
                    or classify_pdf_text(pdf_bytes)
                )
                target = rebuild_api.TARGETS.get(board_key)
                if not target:
                    unmatched.append(f"{source_name} could not be matched.")
                    continue
                if board_key in uploaded_targets:
                    raise ValueError(f"More than one PDF matched {target['label']}. Send one PDF for that board type.")
                uploaded_targets[board_key] = pdf_bytes
                matched.append({
                    "board": board_key,
                    "label": target["label"],
                    "sourceName": source_name,
                    "filename": target["filename"],
                })

            if not uploaded_targets:
                twiml_response(self, f"No booking boards were updated. {' '.join(unmatched)}".strip())
                return

            sender = str(params.get("From") or "").strip()
            recipient = str(params.get("To") or "").strip()
            queued_count = queue_uploads(sender, recipient, matched, uploaded_targets)
            labels = ", ".join(item["label"] for item in matched)
            extra = f" Unmatched: {' '.join(unmatched)}" if unmatched else ""
            twiml_response(self, f"Received {queued_count} PDF(s): {labels}. I will wait {WHATSAPP_BATCH_WAIT_SECONDS} seconds after the last forwarded PDF, then update the booking boards once and confirm here.{extra}")
        except Exception as error:
            twiml_response(self, f"Booking board update failed: {str(error)[:900]}")
