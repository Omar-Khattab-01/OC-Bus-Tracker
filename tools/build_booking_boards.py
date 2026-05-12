from __future__ import annotations

import json
import os
import re
from collections import defaultdict
from pathlib import Path

from pypdf import PdfReader

ROOT = Path(__file__).resolve().parents[1]
SOURCE_DIR = Path(os.environ.get("BOOKING_BOARDS_SOURCE_DIR") or ROOT / "Booking_Boards")
OUTPUT_FILE = Path(os.environ.get("BOOKING_BOARDS_OUTPUT_FILE") or ROOT / "data" / "booking_boards.json")

TIME_RE = re.compile(r"\b\d{2}:\d{2}\b")
BLOCK_RE = re.compile(r"\b\d{1,3}-\d{2}\b")
WIDE_BLOCK_RE = re.compile(r"\b\d{1,3}-\d{2,3}\b")


def extract_lines(pdf_path: Path):
    reader = PdfReader(str(pdf_path))
    pages = []
    for page_index, page in enumerate(reader.pages, start=1):
        lines = []
        for raw_line in (page.extract_text() or "").splitlines():
            line = " ".join(raw_line.split()).strip()
            if line:
                lines.append(line)
        pages.append({"page": page_index, "lines": lines})
    return pages


def extract_position_rows(pdf_path: Path):
    reader = PdfReader(str(pdf_path))
    pages = []
    for page_index, page in enumerate(reader.pages, start=1):
        items = []

        def visitor(text, cm, tm, font_dict, font_size):
            if text and text.strip():
                x = round(tm[4], 1)
                y = round(tm[5], 1)
                items.append((y, x, " ".join(text.split()).strip()))

        page.extract_text(visitor_text=visitor)
        rows = defaultdict(list)
        for y, x, text in items:
            if not text:
                continue
            rows[round(y)].append((x, text))
        structured = []
        for y in sorted(rows.keys(), reverse=True):
            values = sorted(rows[y], key=lambda item: item[0])
            structured.append({
                "y": y,
                "cells": [{"x": x, "text": text} for x, text in values],
            })
        pages.append({"page": page_index, "rows": structured})
    return pages


def parse_weekend_board(pdf_path: Path, board_id: str, service_day: str, title: str):
    line_pages = extract_lines(pdf_path)
    position_pages = extract_position_rows(pdf_path)
    entries = []
    slot_prefix = "sat" if service_day == "saturday" else "sun"

    def make_entry(page_number: int, shift_id: str):
        return {
            "id": f"{board_id}-{service_day}-{page_number}-{shift_id or len(entries) + 1}",
            "title": f"{service_day.title()} board shift",
            "serviceDay": service_day,
            "boardPage": page_number,
            "availabilityStart": "",
            "availabilityEnd": "",
            "taken": False,
            "pieces": [],
            "sourcePdf": pdf_path.name,
            "shiftId": shift_id,
            f"{slot_prefix}1Taken": False,
            f"{slot_prefix}2Taken": False,
        }

    def parse_saturday_piece_line(line: str):
        times = re.findall(r"\d{2}:\d{2}", line)
        if len(times) < 3:
            return None
        start_time, end_time, plat_time = times[-3], times[-2], times[-1]
        times_match = re.search(rf"{start_time}\s+{end_time}\s+{plat_time}$", line)
        if not times_match:
            return None

        prefix = line[:times_match.start()].strip()
        best = None
        for candidate in re.finditer(r"(?=(\d{2,3}-\d{2}))", prefix):
            block = candidate.group(1)
            start = candidate.start(1)
            end = start + len(block)
            left = prefix[:start].strip()
            right = prefix[end:].strip()
            route_match = re.search(r"(\d[\d,]*)$", left)
            if not route_match:
                continue
            route_label = route_match.group(1)
            off_location = left[:route_match.start()].strip()
            shift_match = re.match(r"^(?:(\d{1,4})\s+)?(.+)$", right)
            if not shift_match:
                continue
            shift_id = shift_match.group(1) or ""
            on_location = shift_match.group(2).strip()
            if not on_location or not off_location:
                continue
            score = (
                len(route_label.replace(",", "")),
                1 if shift_id else 0,
                -len(block),
            )
            candidate_data = {
                "pieceId": shift_id,
                "block": block,
                "routeLabel": route_label,
                "from": on_location,
                "to": off_location,
                "startTime": start_time,
                "endTime": end_time,
                "payTime": plat_time,
            }
            if best is None or score > best[0]:
                best = (score, candidate_data)
        return best[1] if best else None

    def parse_sunday_piece_line(line: str):
        match = re.match(
            r"^(?:(?P<shift>\d{1,4})\s+)?(?P<start>\d{2}:\d{2})(?P<end>\d{2}:\d{2})(?P<plat>\d{2}:\d{2})(?P<rest>.+)$",
            line,
        )
        if not match:
            return None

        rest = match.group("rest").strip()
        block_match = re.search(r"(?<!\d)(\d{1,3}-\d{2})(?!\d)", rest)
        if not block_match:
            return None

        on_location = rest[:block_match.start()].strip()
        suffix = rest[block_match.end():].strip()
        route_match = re.search(r"(\d[\d,]*)$", suffix)
        if not route_match:
            return None
        off_location = suffix[:route_match.start()].strip()
        if not on_location or not off_location:
            return None

        return {
            "pieceId": match.group("shift") or "",
            "block": block_match.group(1),
            "routeLabel": route_match.group(1),
            "from": on_location,
            "to": off_location,
            "startTime": match.group("start"),
            "endTime": match.group("end"),
            "payTime": match.group("plat"),
        }

    def extract_weekend_slot_markers(page_rows):
        shift_rows = []
        xx_rows = []
        for row in page_rows:
            xs = [cell["x"] for cell in row["cells"] if cell["text"] == "XX" and cell["x"] > 0]
            if xs:
                xx_rows.append({"y": row["y"], "xs": xs})
            shift_cell = next(
                (
                    cell
                    for cell in row["cells"]
                    if re.fullmatch(r"\d{1,4}", cell["text"]) and 160 <= cell["x"] <= 230
                ),
                None,
            )
            if shift_cell:
                shift_rows.append({"y": row["y"], "shiftId": shift_cell["text"]})

        markers = []
        for index, shift in enumerate(shift_rows):
            upper = shift["y"] + 1
            lower = shift_rows[index + 1]["y"] if index + 1 < len(shift_rows) else -10_000
            relevant_xx = [
                row
                for row in xx_rows
                if lower < row["y"] < upper
            ]
            markers.append({
                f"{slot_prefix}1Taken": any(any(x < 80 for x in row["xs"]) for row in relevant_xx),
                f"{slot_prefix}2Taken": any(any(x >= 80 for x in row["xs"]) for row in relevant_xx),
            })
        return markers

    marker_map_by_page = {
        page["page"]: extract_weekend_slot_markers(page["rows"])
        for page in position_pages
    }

    parser = parse_saturday_piece_line if service_day == "saturday" else parse_sunday_piece_line

    for page in line_pages:
        current_entry = None
        page_markers = marker_map_by_page.get(page["page"], [])
        shift_index = 0

        def flush_current():
            nonlocal current_entry, shift_index
            if current_entry and current_entry.get("pieces"):
                markers = page_markers[shift_index] if shift_index < len(page_markers) else {}
                current_entry.update(markers)
                current_entry["taken"] = bool(
                    current_entry.get(f"{slot_prefix}1Taken") and current_entry.get(f"{slot_prefix}2Taken")
                )
                entries.append(current_entry)
                shift_index += 1
            current_entry = None

        for line in page["lines"]:
            if line.startswith("Page ") or "Mixed Odd Work" in line or line in {"Sat", "Sun", "#1", "#2"}:
                continue
            if re.fullmatch(r"\d{2}:\d{2} \d{2}:\d{2}", line):
                if current_entry:
                    current_entry["availabilityStart"], current_entry["availabilityEnd"] = line.split()
                continue
            piece = parser(line)
            if not piece:
                continue
            if piece["pieceId"]:
                flush_current()
                current_entry = make_entry(page["page"], piece["pieceId"])
            if current_entry is None:
                current_entry = make_entry(page["page"], piece["pieceId"] or str(len(entries) + 1))
            current_entry["pieces"].append({
                **piece,
                "page": page["page"],
                "taken": None,
            })
        flush_current()
    return {
        "id": board_id,
        "title": title,
        "serviceDay": service_day,
        "sourcePdf": pdf_path.name,
        "entries": entries,
    }


DAILY_LINE_RE = re.compile(
    r"^(?P<end_time>\d{2}:\d{2}) (?P<end_loc>.+?) (?P<pay>\d{2}:\d{2})(?P<start_loc>.+?)(?P<block>\d{1,3}-\d{2,3}) (?P<start_time>\d{2}:\d{2})$"
)

DAILY_NORMAL_LINE_RE = re.compile(
    r"^(?P<block>\d{1,3}-\d{2,3}) (?P<start_loc>.+?) (?P<start_time>\d{2}:\d{2}) (?P<end_time>\d{2}:\d{2}) (?P<end_loc>.+?) (?P<pay>\d{2}:\d{2})$"
)

STAT_SHIFT_LINE_RE = re.compile(
    r"^(?P<taken>XXXX\s+)?(?P<shift>\d{1,4}) (?P<block>\d{1,3}-\d{2}) (?P<start_loc>.+?) (?P<start_time>\d{2}:\d{2}) (?P<end_time>\d{2}:\d{2}) (?P<end_loc>.+?) (?P<plat>\d{2}:\d{2})$"
)

STAT_CONTINUATION_LINE_RE = re.compile(
    r"^(?P<block>\d{1,3}-\d{2}) (?P<start_loc>.+?) (?P<start_time>\d{2}:\d{2}) (?P<end_time>\d{2}:\d{2}) (?P<end_loc>.+?) (?P<plat>\d{2}:\d{2})$"
)

WEEKEND_ODD_SAT_LINE_RE = re.compile(
    r"^(?P<start_time>\d{2}:\d{2}) (?P<end_time>\d{2}:\d{2}) (?P<end_loc>.+?) (?P<pay>\d{2}:\d{2})(?P<block>\d{1,3}-\d{2,3}) (?P<start_loc>.+)$"
)

WEEKEND_RELIEF_SAT_LINE_RE = re.compile(
    r"^(?P<start_time>\d{2}:\d{2}) (?P<end_time>\d{2}:\d{2}) (?P<end_loc>.+?) (?P<pay>\d{2}:\d{2})(?P<start_loc>.+)$"
)

WEEKEND_SUN_LINE_RE = re.compile(
    r"^(?P<start_loc>.+?) (?P<pay>\d{2}:\d{2})(?P<end_loc>.+?)(?P<end_time>\d{2}:\d{2})(?P<start_time>\d{2}:\d{2})$"
)


def parse_daily_board(pdf_path: Path):
    pages = extract_lines(pdf_path)
    entries = []
    spare_summary = []
    current = None
    pending_route = ""
    reached_daily_booked_work = False
    current_section = "Daily Open Work"
    daily_section_headers = {"Daily Open Work", "Mixed Odd Work", "Mixed Relief Work"}

    def parse_spare_summary(line: str):
        match = re.fullmatch(r"(General Spare|Holiday Spares|Vacation Spares)\s+(\d+)\s+(\d+)\s+(\d+)", line)
        if not match:
            return None
        title, available, booked, limit = match.groups()
        return {
            "id": re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-"),
            "title": "Vacation Spares" if title == "Holiday Spares" else title,
            "limit": int(limit),
            "booked": int(booked),
            "available": int(available),
        }

    def flush_current():
        nonlocal current, pending_route
        if current and current.get("pieces"):
            entries.append(current)
        current = None
        pending_route = ""

    def append_piece_from_match(match, page_number: int):
        nonlocal pending_route
        if current is None:
            return
        current["pieces"].append({
            "pieceId": current.get("shiftId", ""),
            "block": match.group("block"),
            "routeLabel": pending_route,
            "from": match.group("start_loc").strip(),
            "to": match.group("end_loc").strip(),
            "startTime": match.group("start_time"),
            "endTime": match.group("end_time"),
            "payTime": match.group("pay"),
            "page": page_number,
            "taken": False,
        })
        pending_route = ""

    for page in pages:
        if reached_daily_booked_work:
            break
        for line in page["lines"]:
            if "Daily Booked Work" in line:
                reached_daily_booked_work = True
                break
            if line in daily_section_headers:
                flush_current()
                current_section = line
                continue
            if page["page"] >= 99:
                continue
            parsed_spare_summary = parse_spare_summary(line)
            if parsed_spare_summary:
                if not any(row["id"] == parsed_spare_summary["id"] for row in spare_summary):
                    spare_summary.append(parsed_spare_summary)
                continue
            if line in {"AM Biddable Trippers", "PM Biddable Trippers"} or line.startswith("General Booking") or line in {"On Location On Time Off LocationOff Time PlatRun PayShift", "Shift On Location On Time Off LocationOff Time Plat", "Pay", "AvailableBookedLimit"}:
                continue
            match_daily_id = re.fullmatch(r"Daily(\d{1,4})", line)
            if match_daily_id:
                flush_current()
                shift_id = match_daily_id.group(1)
                current = {
                    "id": f"daily-{page['page']}-{shift_id}",
                    "title": f"Daily {shift_id}",
                    "serviceDay": "weekday",
                    "boardPage": page["page"],
                    "availabilityStart": "",
                    "availabilityEnd": "",
                    "taken": False,
                    "pieces": [],
                    "sourcePdf": pdf_path.name,
                    "shiftId": shift_id,
                    "workSection": current_section,
                }
                pending_route = ""
                continue
            if re.fullmatch(r"Daily", line):
                continue
            if current is None:
                continue
            if re.fullmatch(r"[A-Z0-9,]+", line):
                if current.get("pieces") and not current["pieces"][-1].get("routeLabel"):
                    current["pieces"][-1]["routeLabel"] = line
                else:
                    pending_route = line
                continue
            if re.fullmatch(r"\d{2}:\d{2} \d{2}:\d{2}", line):
                start, end = line.split()
                current["availabilityStart"] = start
                current["availabilityEnd"] = end
                continue
            match = DAILY_LINE_RE.match(line)
            if match:
                append_piece_from_match(match, page["page"])
                continue
            match = DAILY_NORMAL_LINE_RE.match(line)
            if match:
                append_piece_from_match(match, page["page"])
    flush_current()

    # Biddable trippers use a tighter format that extracts more reliably from positioned rows.
    # Keep reading until the Daily Booked Work heading; that section is already assigned work.
    pending_route = ""
    pending_shift = ""
    expecting_daily = False
    last_biddable_entry = None
    in_biddable_section = False
    current_biddable_section = "AM Biddable Trippers"
    for page in extract_position_rows(pdf_path):
        for row in page["rows"]:
            cells = row["cells"]
            texts = [cell["text"] for cell in cells]
            if not texts:
                continue
            if any("Daily Booked Work" in text for text in texts):
                return {
                    "id": "daily_open_work",
                    "title": "Daily Open Work",
                    "serviceDay": "weekday",
                    "sourcePdf": pdf_path.name,
                    "spareSummary": spare_summary,
                    "entries": [entry for entry in entries if entry.get("pieces")],
                }
            if any("AM Biddable Trippers" in text for text in texts):
                current_biddable_section = "AM Biddable Trippers"
                in_biddable_section = True
                expecting_daily = False
                pending_route = ""
                pending_shift = ""
                continue
            if any("PM Biddable Trippers" in text for text in texts):
                current_biddable_section = "PM Biddable Trippers"
                in_biddable_section = True
                expecting_daily = False
                pending_route = ""
                pending_shift = ""
                continue
            if not in_biddable_section:
                continue
            shift_cell = next((cell for cell in cells if re.fullmatch(r"\d{1,4}", cell["text"])), None)
            detail_cell = next((
                cell for cell in cells
                if WIDE_BLOCK_RE.search(cell["text"]) or re.search(r"\d{1,3}-\d{2,3}\d{3,4}$", cell["text"]) or re.search(r"\d{1,3}-\d{2,3}$", cell["text"])
            ), None)
            time_cells = [cell["text"] for cell in cells if TIME_RE.fullmatch(cell["text"])]
            route_text = next((text for text in texts if re.fullmatch(r"[A-Z0-9,]+", text) and text not in {"Daily", "Pay", "Shift", "Run"}), "")
            if expecting_daily and detail_cell and len(time_cells) >= 2:
                route_text = ""
            if route_text:
                if expecting_daily and re.fullmatch(r"\d{3,4}", route_text):
                    pending_shift = route_text
                    continue
                if last_biddable_entry and last_biddable_entry.get("pieces") and not last_biddable_entry["pieces"][-1].get("routeLabel"):
                    last_biddable_entry["pieces"][-1]["routeLabel"] = route_text
                else:
                    pending_route = route_text
                continue
            if any(text == "Daily" for text in texts):
                expecting_daily = True
                continue
            if not expecting_daily:
                continue
            if not shift_cell or not detail_cell:
                if not detail_cell:
                    continue
                suffix_match = re.search(r"(?P<block>\d{1,3}-\d{2,3})(?P<shift>\d{3,4})$", detail_cell["text"])
                if not suffix_match and not pending_shift:
                    continue
                raw_shift_id = suffix_match.group("shift") if suffix_match else pending_shift
                shift_id = raw_shift_id.lstrip("0") or raw_shift_id
            else:
                shift_id = shift_cell["text"]
            detail_text = detail_cell["text"]
            block_match = re.search(r"(?P<block>\d{1,3}-\d{2,3})(?P<shift>\d{3,4})$", detail_text)
            if not block_match:
                block_match = re.search(r"(?P<block>\d{1,3}-\d{2,3})$", detail_text)
            if not block_match:
                block_match = WIDE_BLOCK_RE.search(detail_text)
            if not block_match or len(time_cells) < 2:
                continue
            block = block_match.group("block") if "block" in block_match.groupdict() else block_match.group(0)
            before_block = detail_text[:block_match.start()]
            after_block = detail_text[block_match.end():]
            before_times = list(re.finditer(r"\d{2}:\d{2}", before_block))
            if len(before_times) < 2:
                continue
            end_time = before_times[0].group(0)
            start_time = before_times[1].group(0)
            end_loc = before_block[:before_times[0].start()].strip()
            start_loc = before_block[before_times[1].end():].strip()
            entry = {
                "id": f"daily-{page['page']}-{shift_id}",
                "title": f"Daily {shift_id}",
                "serviceDay": "weekday",
                "boardPage": page["page"],
                "availabilityStart": "",
                "availabilityEnd": "",
                "taken": False,
                "pieces": [{
                    "pieceId": shift_id,
                    "block": block,
                    "routeLabel": pending_route,
                    "from": start_loc,
                    "to": end_loc,
                    "startTime": start_time,
                    "endTime": end_time,
                    "payTime": time_cells[0],
                    "page": page["page"],
                    "taken": False,
                }],
                "sourcePdf": pdf_path.name,
                "shiftId": shift_id,
                "workSection": current_biddable_section,
            }
            entries.append(entry)
            last_biddable_entry = entry
            expecting_daily = False
            pending_route = ""
            pending_shift = ""

    return {
        "id": "daily_open_work",
        "title": "Daily Open Work",
        "serviceDay": "weekday",
        "sourcePdf": pdf_path.name,
        "spareSummary": spare_summary,
        "entries": [entry for entry in entries if entry.get("pieces")],
    }


def parse_bus_summer_weekend_board(pdf_path: Path):
    pages = extract_lines(pdf_path)
    entries = []
    current = None
    pending_route = ""
    pending_block = ""
    current_section = "Mixed Odd Work Saturday"
    current_service_day = "saturday"
    expecting_shift = False

    def flush_current():
        nonlocal current, pending_route, pending_block
        if current and current.get("pieces"):
            entries.append(current)
        current = None
        pending_route = ""
        pending_block = ""

    def set_section(line: str):
        nonlocal current_section, current_service_day
        normalized = line.lower()
        current_section = line
        current_service_day = "sunday" if "sunday" in normalized else "saturday"

    def make_entry(page_number: int, shift_line: str):
        taken = "XXXX" in shift_line
        shift_id = re.sub(r"\D", "", shift_line) or str(len(entries) + 1)
        return {
            "id": f"bus-weekend-{page_number}-{shift_id}",
            "title": f"Weekend {shift_id}",
            "serviceDay": current_service_day,
            "boardPage": page_number,
            "availabilityStart": "",
            "availabilityEnd": "",
            "taken": taken,
            "pieces": [],
            "sourcePdf": pdf_path.name,
            "shiftId": shift_id,
            "workSection": current_section,
            "sat1Taken": current_service_day == "saturday" and taken,
            "sat2Taken": current_service_day == "saturday" and "XXXXXXXX" in shift_line,
            "sun1Taken": current_service_day == "sunday" and taken,
            "sun2Taken": current_service_day == "sunday" and "XXXXXXXX" in shift_line,
        }

    def append_piece(piece, page_number: int):
        nonlocal pending_route
        if current is None:
            return
        current["pieces"].append({
            **piece,
            "pieceId": current.get("shiftId", ""),
            "routeLabel": pending_route,
            "page": page_number,
            "taken": current.get("taken", False),
        })
        pending_route = ""

    for page in pages:
        for line in page["lines"]:
            if line in {"Mixed Odd Work Saturday", "Mixed Relief Work Saturday", "Mix Odd Work Sunday", "Mixed Relief Work Sunday"}:
                flush_current()
                set_section(line)
                expecting_shift = False
                continue
            if (
                line.startswith("5/")
                or line.startswith("Off Location")
                or line.startswith("Plat Pay")
                or line.startswith("PayPlat")
                or line.startswith("On Location")
                or line.startswith("General Booking")
            ):
                continue
            if line in {"SAT1 SAT2", "SUN1 SUN2"}:
                flush_current()
                expecting_shift = True
                continue
            shift_match = re.fullmatch(r"(?:X{4,8})?\d{1,4}(?:\s+X{4,8})?", line)
            if expecting_shift and shift_match:
                flush_current()
                current = make_entry(page["page"], line)
                expecting_shift = False
                continue
            if current is None:
                continue
            if re.fullmatch(r"\d{2}:\d{2} \d{2}:\d{2}", line):
                current["availabilityStart"], current["availabilityEnd"] = line.split()
                continue
            if WIDE_BLOCK_RE.fullmatch(line):
                pending_block = line
                continue
            if re.fullmatch(r"[A-Z0-9,]+", line):
                pending_route = line
                continue

            piece = None
            sat_odd = WEEKEND_ODD_SAT_LINE_RE.match(line)
            if sat_odd:
                piece = {
                    "block": sat_odd.group("block"),
                    "from": sat_odd.group("start_loc").strip(),
                    "to": sat_odd.group("end_loc").strip(),
                    "startTime": sat_odd.group("start_time"),
                    "endTime": sat_odd.group("end_time"),
                    "payTime": sat_odd.group("pay"),
                }
            else:
                sat_relief = WEEKEND_RELIEF_SAT_LINE_RE.match(line)
                if sat_relief and pending_block:
                    piece = {
                        "block": pending_block,
                        "from": sat_relief.group("start_loc").strip(),
                        "to": sat_relief.group("end_loc").strip(),
                        "startTime": sat_relief.group("start_time"),
                        "endTime": sat_relief.group("end_time"),
                        "payTime": sat_relief.group("pay"),
                    }
                else:
                    sunday = WEEKEND_SUN_LINE_RE.match(line)
                    if sunday and pending_block:
                        piece = {
                            "block": pending_block,
                            "from": sunday.group("start_loc").strip(),
                            "to": sunday.group("end_loc").strip(),
                            "startTime": sunday.group("start_time"),
                            "endTime": sunday.group("end_time"),
                            "payTime": sunday.group("pay"),
                        }
            if piece:
                append_piece(piece, page["page"])

    flush_current()
    return {
        "id": "weekend_boards",
        "title": "Weekend Work",
        "serviceDay": "weekend",
        "sourcePdf": pdf_path.name,
        "entries": [entry for entry in entries if entry.get("pieces")],
    }


def parse_days_off_counter(pdf_path: Path):
    lines = extract_lines(pdf_path)[0]["lines"]
    rows = []
    day_names = {"Monday", "Tuesday", "Wednesday", "Wedneday", "Thursday", "Friday"}
    for line in lines:
        parts = line.split()
        if not parts or parts[0] not in day_names:
            continue
        day = "Wednesday" if parts[0] == "Wedneday" else parts[0]
        nums = parts[1:]
        week = ""
        total = booked = remaining = None
        if len(nums) == 4:
            week, total, booked, remaining = nums
        elif len(nums) == 3:
            packed, booked, remaining = nums
            if len(packed) > 2 and packed[0] in {"1", "2"}:
                week = packed[0]
                total = packed[1:]
            else:
                total = packed
        if total is None:
            continue
        rows.append({
            "day": day,
            "week": week,
            "total": int(total),
            "booked": int(booked),
            "remaining": int(remaining),
        })

    counters = []
    for title, offset in (("Day Work", 0), ("Night Work", 10)):
        counter_rows = rows[offset:offset + 10]
        counters.append({
            "id": title.lower().replace(" ", "_"),
            "title": title,
            "rows": counter_rows,
            "total": sum(row["total"] for row in counter_rows),
            "booked": sum(row["booked"] for row in counter_rows),
            "remaining": sum(row["remaining"] for row in counter_rows),
        })

    return {
        "id": "days_off_counter",
        "title": "Days Off Counter",
        "serviceDay": "weekday",
        "sourcePdf": pdf_path.name,
        "entries": [],
        "counters": counters,
    }


def parse_stat_board(pdf_path: Path):
    pages = extract_lines(pdf_path)
    entries = []
    current = None
    pending_route = ""

    def normalize_stat_holiday(line: str):
        normalized = line.lower()
        if "canada day" in normalized:
            return "canada-day", "Canada Day"
        if "august civic" in normalized:
            return "august-civic", "August Civic"
        return "", ""

    def flush_current():
        nonlocal current, pending_route
        if current and current.get("pieces"):
            entries.append(current)
        current = None
        pending_route = ""

    for page in pages:
        holiday_key = ""
        holiday_label = ""
        for line in page["lines"]:
            parsed_holiday_key, parsed_holiday_label = normalize_stat_holiday(line)
            if parsed_holiday_key:
                holiday_key = parsed_holiday_key
                holiday_label = parsed_holiday_label
                continue
            if line.startswith("Mixed Odd Work") or line in {"Pay", "Avail", "On Location On Time Plat PayOff LocationOff TimeRunShiftAvail"}:
                continue
            if re.fullmatch(r"\d{2}:\d{2} \d{2}:\d{2}", line):
                if current:
                    current["availabilityStart"], current["availabilityEnd"] = line.split()
                continue
            if re.fullmatch(r"[\d,]+", line):
                pending_route = line
                continue
            shift_match = STAT_SHIFT_LINE_RE.match(line)
            if shift_match:
                route_label = pending_route
                flush_current()
                current = {
                    "id": f"stat-{page['page']}-{len(entries)+1}",
                    "title": "Stat work",
                    "serviceDay": "special",
                    "boardPage": page["page"],
                    "availabilityStart": "",
                    "availabilityEnd": "",
                    "taken": bool(shift_match.group("taken")),
                    "pieces": [{
                        "pieceId": shift_match.group("shift"),
                        "block": shift_match.group("block"),
                        "routeLabel": route_label,
                        "from": shift_match.group("start_loc").strip(),
                        "to": shift_match.group("end_loc").strip(),
                        "startTime": shift_match.group("start_time"),
                        "endTime": shift_match.group("end_time"),
                        "payTime": shift_match.group("plat"),
                        "page": page["page"],
                        "taken": bool(shift_match.group("taken")),
                    }],
                    "sourcePdf": pdf_path.name,
                    "shiftId": shift_match.group("shift"),
                    "holidayKey": holiday_key,
                    "holidayLabel": holiday_label,
                }
                pending_route = ""
                continue
            continuation_match = STAT_CONTINUATION_LINE_RE.match(line)
            if continuation_match and current:
                current["pieces"].append({
                    "pieceId": current.get("shiftId", ""),
                    "block": continuation_match.group("block"),
                    "routeLabel": pending_route,
                    "from": continuation_match.group("start_loc").strip(),
                    "to": continuation_match.group("end_loc").strip(),
                    "startTime": continuation_match.group("start_time"),
                    "endTime": continuation_match.group("end_time"),
                    "payTime": continuation_match.group("plat"),
                    "page": page["page"],
                    "taken": current.get("taken", False),
                })
                pending_route = ""
        flush_current()
    return {
        "id": "stat_work",
        "title": "Stat Work",
        "serviceDay": "special",
        "sourcePdf": pdf_path.name,
        "entries": entries,
    }


def parse_spares_board(pdf_path: Path, board_id: str = "spares", title: str = "Spare Boards"):
    pages = extract_lines(pdf_path)
    sections = []
    current_section = None
    current_garage = None

    def parse_spare_row(line: str):
      compact_match = re.fullmatch(r"(\d+)\s+(\d+)\s+(\d+)(\d{2}:\d{2})", line)
      if compact_match:
          limit, booked, available, on_time = compact_match.groups()
          return {
              "onTime": on_time,
              "limit": int(limit),
              "booked": int(booked),
              "available": int(available),
          }
      match = re.fullmatch(r"(\d{2}:\d{2})\s+(\d+)\s+(\d+)\s+(\d+)", line)
      if match:
          on_time, limit, booked, available = match.groups()
          return {
              "onTime": on_time,
              "limit": int(limit),
              "booked": int(booked),
              "available": int(available),
          }
      return None

    def flush_section():
      nonlocal current_section, current_garage
      if current_section and current_section.get("garages"):
          sections.append(current_section)
      current_section = None
      current_garage = None

    def start_section(title: str, page_number: int):
      nonlocal current_section, current_garage
      flush_section()
      current_section = {
          "id": re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-"),
          "title": title,
          "page": page_number,
          "garages": [],
      }
      current_garage = None

    def ensure_garage(name: str):
      nonlocal current_garage
      if current_section is None:
          return None
      existing = next((garage for garage in current_section["garages"] if garage["name"] == name), None)
      if existing:
          current_garage = existing
          return existing
      garage = {"name": name, "slots": []}
      current_section["garages"].append(garage)
      current_garage = garage
      return garage

    for page in pages:
        for line in page["lines"]:
            if line.startswith("General Booking Spare Progress Report") or re.fullmatch(r"\d+/\d+/\d+.*", line):
                continue
            if line in {"Booked AvailableLimitOn Time", "On Time Limit Booked Available"}:
                continue
            if line in {"Saturday Spare", "Sunday Spare", "Stats Spare"}:
                flush_section()
                continue
            if "Spare" in line or line.startswith("Canada Day ") or line.startswith("August Civic "):
                start_section(line, page["page"])
                continue
            row = parse_spare_row(line)
            if row:
                garage = current_garage or ensure_garage("All locations")
                if garage is not None:
                    garage["slots"].append(row)
                continue
            if current_section is not None:
                ensure_garage(line)
        # keep current section open across page breaks for continued rows
    flush_section()
    return {
        "id": board_id,
        "title": title,
        "serviceDay": "mixed",
        "sourcePdf": pdf_path.name,
        "entries": [],
        "sections": sections,
    }


def main():
    boards = []
    boards.append(parse_daily_board(SOURCE_DIR / "2026 Summer daily bords.pdf"))
    boards.append(parse_bus_summer_weekend_board(SOURCE_DIR / "2026 Bus Summer Weekend boards.pdf"))
    boards.append(parse_days_off_counter(SOURCE_DIR / "2026 Summer Days off Counter (3).pdf"))
    boards.append(parse_spares_board(SOURCE_DIR / "2026 Summer Spare,s Boards.pdf"))
    boards.append(parse_spares_board(SOURCE_DIR / "2026 Summer Daily and weekly floating spares  (4) (1).pdf", "floating_spares", "Daily and Weekly Floating Spares"))
    boards.append(parse_stat_board(SOURCE_DIR / "2026 Summer stat work.pdf"))
    payload = {"generatedFrom": "Booking_Boards PDFs", "boards": boards}
    OUTPUT_FILE.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Wrote {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
