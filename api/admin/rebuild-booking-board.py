from __future__ import annotations

import base64
import hmac
import importlib.util
import json
import os
import shutil
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SOURCE_DIR = ROOT / "Booking_Boards"
BUILD_SCRIPT = ROOT / "tools" / "build_booking_boards.py"
TARGETS = {
    "daily": {
        "label": "Daily Work",
        "filename": "2026 Summer daily bords.pdf",
        "board_ids": ["daily_open_work"],
    },
    "weekend": {
        "label": "Weekend Work",
        "filename": "2026 Bus Summer Weekend boards.pdf",
        "board_ids": ["weekend_boards"],
    },
    "spares": {
        "label": "Spare Boards",
        "filename": "2026 Summer Spare,s Boards.pdf",
        "board_ids": ["spares"],
    },
    "floating_spares": {
        "label": "Daily and Weekly Floating Spares",
        "filename": "2026 Summer Daily and weekly floating spares  (4) (1).pdf",
        "board_ids": ["floating_spares"],
    },
    "days_off_counter": {
        "label": "Days Off Counter",
        "filename": "2026 Summer Days off Counter (3).pdf",
        "board_ids": ["days_off_counter"],
    },
    "stat": {
        "label": "Stat Work",
        "filename": "2026 Summer stat work.pdf",
        "board_ids": ["stat_work"],
    },
}


def normalize_upload_name(name: str) -> str:
    return " ".join(str(name or "").replace("_", " ").replace("-", " ").lower().split())


def classify_upload_name(name: str) -> str:
    normalized = normalize_upload_name(name)
    if "floating" in normalized:
        return "floating_spares"
    if "days off" in normalized or "day off" in normalized or "counter" in normalized:
        return "days_off_counter"
    if "weekend" in normalized or "saturday" in normalized or "sunday" in normalized:
        return "weekend"
    if "stat" in normalized or "canada day" in normalized or "august civic" in normalized:
        return "stat"
    if "spare" in normalized:
        return "spares"
    if "daily" in normalized or "bords" in normalized or "board" in normalized:
        return "daily"
    return ""


def json_response(handler: BaseHTTPRequestHandler, status: int, payload: dict):
    body = json.dumps(payload).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def header_value(handler: BaseHTTPRequestHandler, name: str) -> str:
    return str(handler.headers.get(name, "")).strip()


def is_authorized(handler: BaseHTTPRequestHandler) -> bool:
    expected = os.environ.get("BOOKING_BOARD_ADMIN_TOKEN", "").strip()
    provided = header_value(handler, "x-admin-token")
    return bool(expected and provided and hmac.compare_digest(expected, provided))


def load_builder():
    spec = importlib.util.spec_from_file_location("booking_board_builder", BUILD_SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not load booking board builder.")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def copy_sources(temp_source_dir: Path):
    temp_source_dir.mkdir(parents=True, exist_ok=True)
    for target in TARGETS.values():
        source_path = SOURCE_DIR / target["filename"]
        if source_path.exists():
            shutil.copyfile(source_path, temp_source_dir / target["filename"])


def build_payload(temp_source_dir: Path) -> dict:
    builder = load_builder()
    daily_pdf = temp_source_dir / "2026 Summer daily bords.pdf"
    weekend_pdf = temp_source_dir / "2026 Bus Summer Weekend boards.pdf"
    days_off_pdf = temp_source_dir / "2026 Summer Days off Counter (3).pdf"
    spares_pdf = temp_source_dir / "2026 Summer Spare,s Boards.pdf"
    floating_spares_pdf = temp_source_dir / "2026 Summer Daily and weekly floating spares  (4) (1).pdf"
    stat_pdf = temp_source_dir / "2026 Summer stat work.pdf"
    boards = [
        builder.with_board_updated_at(builder.parse_daily_board(daily_pdf), daily_pdf),
        builder.with_board_updated_at(builder.parse_bus_summer_weekend_board(weekend_pdf), weekend_pdf),
        builder.with_board_updated_at(builder.parse_days_off_counter(days_off_pdf), days_off_pdf),
        builder.with_board_updated_at(builder.parse_spares_board(spares_pdf), spares_pdf),
        builder.with_board_updated_at(builder.parse_spares_board(
            floating_spares_pdf,
            "floating_spares",
            "Daily and Weekly Floating Spares",
        ), floating_spares_pdf),
        builder.with_board_updated_at(builder.parse_stat_board(stat_pdf), stat_pdf),
    ]
    return {
        "generatedFrom": "Booking_Boards PDFs",
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "boards": boards,
    }


def github_request(method: str, url: str, token: str, body: dict | None = None) -> dict:
    data = json.dumps(body).encode("utf-8") if body is not None else None
    request = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "User-Agent": "oc-bus-tracker-booking-board-admin",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            raw = response.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"GitHub {error.code}: {detail[:500]}") from error


def get_github_file(repo: str, branch: str, path: str, token: str) -> dict:
    encoded_path = urllib.parse.quote(path)
    url = f"https://api.github.com/repos/{repo}/contents/{encoded_path}?ref={urllib.parse.quote(branch)}"
    return github_request("GET", url, token)


def put_github_file(repo: str, branch: str, path: str, content: bytes, message: str, token: str):
    existing = get_github_file(repo, branch, path, token)
    encoded_path = urllib.parse.quote(path)
    url = f"https://api.github.com/repos/{repo}/contents/{encoded_path}"
    github_request(
        "PUT",
        url,
        token,
        {
            "message": message,
            "branch": branch,
            "content": base64.b64encode(content).decode("ascii"),
            "sha": existing.get("sha"),
        },
    )


def get_existing_booking_board_payload(repo: str, branch: str, token: str) -> dict:
    try:
        existing = get_github_file(repo, branch, "data/booking_boards.json", token)
        encoded = str(existing.get("content") or "")
        if not encoded:
            return {}
        return json.loads(base64.b64decode(encoded).decode("utf-8"))
    except Exception:
        return {}


def fallback_updated_at(payload: dict) -> str:
    return str(payload.get("updatedAt") or payload.get("generatedAt") or "").strip()


def merge_board_update_timestamps(payload: dict, previous_payload: dict, target: dict, updated_at: str) -> dict:
    targets = target if isinstance(target, list) else [target]
    target_board_ids = {
        board_id
        for item in targets
        for board_id in (item.get("board_ids") or [])
    }
    previous_fallback = fallback_updated_at(previous_payload)
    previous_by_id = {
        str(board.get("id") or "").strip(): board
        for board in previous_payload.get("boards", [])
        if str(board.get("id") or "").strip()
    }
    boards = []
    for board in payload.get("boards", []):
        board_id = str(board.get("id") or "").strip()
        previous_board = previous_by_id.get(board_id, {})
        boards.append({
            **board,
            "updatedAt": updated_at
            if board_id in target_board_ids
            else str(previous_board.get("updatedAt") or previous_fallback or board.get("updatedAt") or "").strip(),
        })
    return {**payload, "boards": boards}


def commit_updates(uploaded_targets: dict[str, bytes], payload: dict):
    token = os.environ.get("BOOKING_BOARD_GITHUB_TOKEN", "").strip()
    if not token:
        raise RuntimeError("Set BOOKING_BOARD_GITHUB_TOKEN in Vercel to permanently update booking boards.")

    repo = os.environ.get("BOOKING_BOARD_GITHUB_REPO", "Omar-Khattab-01/OC-Bus-Tracker").strip()
    branch = os.environ.get("BOOKING_BOARD_GITHUB_BRANCH", "main").strip()
    updated_at = datetime.now(timezone.utc).isoformat()
    previous_payload = get_existing_booking_board_payload(repo, branch, token)
    targets = [TARGETS[key] for key in uploaded_targets.keys()]
    payload = merge_board_update_timestamps(payload, previous_payload, targets, updated_at)
    data_bytes = json.dumps(payload, indent=2).encode("utf-8") + b"\n"

    for board_key, pdf_bytes in uploaded_targets.items():
        target = TARGETS[board_key]
        pdf_path = f"Booking_Boards/{target['filename']}"
        put_github_file(
            repo,
            branch,
            pdf_path,
            pdf_bytes,
            f"Update {target['label']} booking board PDF",
            token,
        )
    put_github_file(
        repo,
        branch,
        "data/booking_boards.json",
        data_bytes,
        f"Rebuild booking boards after batch upload",
        token,
    )
    return repo, branch, updated_at, payload


def decode_batch_files(raw_body: bytes) -> tuple[dict[str, bytes], list[dict], list[str]]:
    try:
        body = json.loads(raw_body.decode("utf-8"))
    except Exception as error:
        raise ValueError("Upload JSON could not be read.") from error

    files = body.get("files") if isinstance(body, dict) else None
    if not isinstance(files, list) or not files:
        raise ValueError("Choose at least one PDF file.")

    uploaded_targets: dict[str, bytes] = {}
    matched = []
    unmatched = []
    for item in files:
        if not isinstance(item, dict):
            continue
        original_name = str(item.get("name") or "").strip()
        board_key = classify_upload_name(original_name)
        if not board_key or board_key not in TARGETS:
            unmatched.append(original_name or "Unnamed PDF")
            continue
        if board_key in uploaded_targets:
            raise ValueError(f"More than one file matched {TARGETS[board_key]['label']}. Keep one PDF for that board.")
        raw_data = str(item.get("data") or "").strip()
        if "," in raw_data and raw_data.lower().startswith("data:"):
            raw_data = raw_data.split(",", 1)[1]
        try:
            pdf_bytes = base64.b64decode(raw_data, validate=True)
        except Exception as error:
            raise ValueError(f"{original_name or TARGETS[board_key]['label']} is not valid base64 PDF data.") from error
        if not pdf_bytes.startswith(b"%PDF-"):
            raise ValueError(f"{original_name or TARGETS[board_key]['label']} is not a valid PDF.")
        uploaded_targets[board_key] = pdf_bytes
        matched.append({
            "board": board_key,
            "label": TARGETS[board_key]["label"],
            "sourceName": original_name,
            "filename": TARGETS[board_key]["filename"],
        })

    if not uploaded_targets:
        raise ValueError("None of the selected PDFs matched a booking board type.")
    return uploaded_targets, matched, unmatched


class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        if not is_authorized(self):
            json_response(self, 401, {"ok": False, "error": "Invalid admin token."})
            return

        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)
        board_key = str(params.get("board", [""])[0]).strip().lower()

        try:
            length = int(header_value(self, "content-length") or "0")
        except ValueError:
            length = 0
        raw_body = self.rfile.read(length)

        try:
            matched = []
            unmatched = []
            uploaded_targets: dict[str, bytes] = {}
            if "application/json" in header_value(self, "content-type").lower():
                uploaded_targets, matched, unmatched = decode_batch_files(raw_body)
            else:
                target = TARGETS.get(board_key)
                if not target:
                    json_response(self, 400, {"ok": False, "error": "Unknown booking board type."})
                    return
                if not raw_body.startswith(b"%PDF-"):
                    json_response(self, 400, {"ok": False, "error": f"Upload a valid PDF file for {target['label']}."})
                    return
                uploaded_targets = {board_key: raw_body}
                matched = [{
                    "board": board_key,
                    "label": target["label"],
                    "sourceName": header_value(self, "x-upload-filename") or target["filename"],
                    "filename": target["filename"],
                }]

            with tempfile.TemporaryDirectory(prefix="booking-boards-") as temp_root:
                temp_source_dir = Path(temp_root) / "Booking_Boards"
                copy_sources(temp_source_dir)
                for key, pdf_bytes in uploaded_targets.items():
                    (temp_source_dir / TARGETS[key]["filename"]).write_bytes(pdf_bytes)
                payload = build_payload(temp_source_dir)
                repo, branch, updated_at, payload = commit_updates(uploaded_targets, payload)

            json_response(
                self,
                200,
                {
                    "ok": True,
                    "uploaded": matched,
                    "missing": [
                        {"board": key, "label": target["label"]}
                        for key, target in TARGETS.items()
                        if key not in uploaded_targets
                    ],
                    "unmatched": unmatched,
                    "boardCount": len(payload["boards"]),
                    "updatedAt": updated_at,
                    "storage": "github",
                    "repo": repo,
                    "branch": branch,
                    "note": "Committed uploaded PDFs and rebuilt data to GitHub. Vercel will redeploy the updated boards from the repository.",
                },
            )
        except ValueError as error:
            json_response(
                self,
                400,
                {
                    "ok": False,
                    "error": str(error)[:900],
                },
            )
        except Exception as error:
            json_response(
                self,
                500,
                {
                    "ok": False,
                    "error": f"Failed while rebuilding booking boards: {str(error)[:900]}",
                },
            )
