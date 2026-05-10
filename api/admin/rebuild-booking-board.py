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
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SOURCE_DIR = ROOT / "Booking_Boards"
BUILD_SCRIPT = ROOT / "tools" / "build_booking_boards.py"
TARGETS = {
    "daily": {
        "label": "Daily Work",
        "filename": "2026 Summer daily bords.pdf",
    },
    "weekend": {
        "label": "Weekend Work",
        "filename": "2026 Bus Summer Weekend boards.pdf",
    },
    "spares": {
        "label": "Spare Boards",
        "filename": "2026 Summer Spare,s Boards.pdf",
    },
    "days_off_counter": {
        "label": "Days Off Counter",
        "filename": "2026 Summer Days off Counter (3).pdf",
    },
    "stat": {
        "label": "Stat Work",
        "filename": "2026 Summer stat work.pdf",
    },
}


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
    boards = [
        builder.parse_daily_board(temp_source_dir / "2026 Summer daily bords.pdf"),
        builder.parse_bus_summer_weekend_board(temp_source_dir / "2026 Bus Summer Weekend boards.pdf"),
        builder.parse_days_off_counter(temp_source_dir / "2026 Summer Days off Counter (3).pdf"),
        builder.parse_spares_board(temp_source_dir / "2026 Summer Spare,s Boards.pdf"),
        builder.parse_stat_board(temp_source_dir / "2026 Summer stat work.pdf"),
    ]
    return {"generatedFrom": "Booking_Boards PDFs", "boards": boards}


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


def commit_updates(target: dict, pdf_bytes: bytes, payload: dict):
    token = os.environ.get("BOOKING_BOARD_GITHUB_TOKEN", "").strip()
    if not token:
        raise RuntimeError("Set BOOKING_BOARD_GITHUB_TOKEN in Vercel to permanently update booking boards.")

    repo = os.environ.get("BOOKING_BOARD_GITHUB_REPO", "Omar-Khattab-01/OC-Bus-Tracker").strip()
    branch = os.environ.get("BOOKING_BOARD_GITHUB_BRANCH", "main").strip()
    pdf_path = f"Booking_Boards/{target['filename']}"
    data_bytes = json.dumps(payload, indent=2).encode("utf-8") + b"\n"

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
        f"Rebuild booking boards after {target['label']} upload",
        token,
    )
    return repo, branch


class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        if not is_authorized(self):
            json_response(self, 401, {"ok": False, "error": "Invalid admin token."})
            return

        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)
        board_key = str(params.get("board", [""])[0]).strip().lower()
        target = TARGETS.get(board_key)
        if not target:
            json_response(self, 400, {"ok": False, "error": "Unknown booking board type."})
            return

        try:
            length = int(header_value(self, "content-length") or "0")
        except ValueError:
            length = 0
        pdf_bytes = self.rfile.read(length)
        if not pdf_bytes.startswith(b"%PDF-"):
            json_response(self, 400, {"ok": False, "error": f"Upload a valid PDF file for {target['label']}."})
            return

        try:
            with tempfile.TemporaryDirectory(prefix="booking-boards-") as temp_root:
                temp_source_dir = Path(temp_root) / "Booking_Boards"
                copy_sources(temp_source_dir)
                (temp_source_dir / target["filename"]).write_bytes(pdf_bytes)
                payload = build_payload(temp_source_dir)
                repo, branch = commit_updates(target, pdf_bytes, payload)

            json_response(
                self,
                200,
                {
                    "ok": True,
                    "board": board_key,
                    "label": target["label"],
                    "filename": target["filename"],
                    "boardCount": len(payload["boards"]),
                    "storage": "github",
                    "repo": repo,
                    "branch": branch,
                    "note": "Committed the PDF and rebuilt data to GitHub. Vercel will redeploy the updated boards from the repository.",
                },
            )
        except Exception as error:
            json_response(
                self,
                500,
                {
                    "ok": False,
                    "error": f"Failed while rebuilding {target['label']} from {target['filename']}: {str(error)[:900]}",
                },
            )
