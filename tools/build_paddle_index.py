import json
import os
import re
from pathlib import Path

from pypdf import PdfReader


ROOT = Path(__file__).resolve().parents[1]
PADDLES_DIR = ROOT / "paddles"
OUTPUT_PATH = ROOT / "data" / "paddles.index.json"

PDF_SOURCES = {
    "Weekdays(5-01 to 60-05).pdf": {
        "source_id": "weekday_regular_low",
        "label": "Weekdays 5-01 to 60-05",
        "service_day": "weekday",
        "category": "regular",
    },
    "Weekdays(61-01 to 899-01).pdf": {
        "source_id": "weekday_regular_high",
        "label": "Weekdays 61-01 to 899-01",
        "service_day": "weekday",
        "category": "regular",
    },
    "ExpressAnd900s(AM & PM).pdf": {
        "source_id": "weekday_express_900",
        "label": "Express and 900 series (AM and PM)",
        "service_day": "weekday",
        "category": "express_900",
    },
    "Saturdays.pdf": {
        "source_id": "saturday_main",
        "label": "Saturday paddles",
        "service_day": "saturday",
        "category": "regular",
    },
    "Sundays.pdf": {
        "source_id": "sunday_main",
        "label": "Sunday paddles",
        "service_day": "sunday",
        "category": "regular",
    },
    "Easter_Monday/Reduced week(5-01 to 60-05).pdf": {
        "source_id": "easter_monday_low",
        "label": "Easter Monday reduced 5-01 to 60-05",
        "service_day": "easter_monday",
        "category": "reduced_week",
        "service_header": "Reduced Week",
    },
    "Easter_Monday/Reduced weeks(61-01 to 899-04).pdf": {
        "source_id": "easter_monday_high",
        "label": "Easter Monday reduced 61-01 to 899-04",
        "service_day": "easter_monday",
        "category": "reduced_week",
        "service_header": "Reduced Week",
    },
    "Easter_Monday/Reduced weeks (AM & PM).pdf": {
        "source_id": "easter_monday_express",
        "label": "Easter Monday reduced AM and PM",
        "service_day": "easter_monday",
        "category": "reduced_week",
        "service_header": "Reduced Week",
    },
}

INSTRUCTION_PREFIXES = (
    "NOTE:",
    "CONTINUE",
    "ROADWAY",
    "ALLOW ",
    "RETURN ",
    "FOLLOW ",
    "PLEASE REFER",
    "THIS IS ",
    "IF MORE THAN ",
    "AVOID ",
    "LORD BYNG",
    "QUEEN,",
    "RIDEAU,",
    "TRANSITWAY ",
    "EXIT ",
    "ENTER ",
)


def clean_line(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def split_csv_routes(value: str):
    return [part.strip() for part in value.split(",") if part.strip()]


def paddle_id_to_block_label(paddle_id: str) -> str:
    route = paddle_id[:3].lstrip("0") or "0"
    run = str(int(paddle_id[3:6]))
    return f"{route}-{run}"


def parse_trip_header(line: str, routes):
    text = line.strip()
    if re.match(r"^\d{1,2}:\d{2}\b", text):
        return None
    for route in sorted(routes, key=len, reverse=True):
        if not text.endswith(route):
            continue
        rest = text[:-len(route)].strip()
        max_trip_len = min(3, len(rest) - 1)
        for trip_len in range(1, max_trip_len + 1):
            trip_digits = rest[:trip_len]
            if not trip_digits.isdigit():
                continue
            trip_number = int(trip_digits)
            if trip_number > 999:
                continue
            headsign = rest[trip_len:].strip()
            if not headsign:
                continue
            if re.match(r"^\d+[A-Za-z]", headsign):
                continue
            return {
                "trip_number": trip_number,
                "headsign": headsign,
                "route": route,
            }
    return None


def parse_trip_header_at(lines, index, routes):
    if index >= len(lines):
        return None, 0

    max_parts = min(3, len(lines) - index)
    for parts in range(1, max_parts + 1):
        if parts > 1:
            next_line = clean_line(lines[index + parts - 1])
            if parse_trip_header(next_line, routes):
                break
        candidate = clean_line(" ".join(lines[index:index + parts]))
        header = parse_trip_header(candidate, routes)
        if header:
            return header, parts

    return None, 0


def looks_like_instruction(line: str) -> bool:
    upper = line.upper()
    if any(upper.startswith(prefix) for prefix in INSTRUCTION_PREFIXES):
        return True
    if "," in line and not re.search(r"\d{1,2}:\d{2}", line):
        return True
    return False


def parse_stop_times(section_lines):
    stop_times = []
    pending_fragments = []
    i = 0

    def flush_pending():
        pending_fragments.clear()

    while i < len(section_lines):
        line = clean_line(section_lines[i])
        if not line:
            i += 1
            continue

        time_only = re.match(r"^(\d{1,2}:\d{2})(?:\s+(Relief))?$", line)
        full_line = re.match(r"^(.*?)(\d{1,2}:\d{2})(?:\s+(Relief))?$", line)

        if time_only and pending_fragments:
            stop_times.append({
                "stop": clean_line(" ".join(pending_fragments)),
                "time": time_only.group(1),
                "relief": bool(time_only.group(2)),
            })
            flush_pending()
            i += 1
            continue

        if full_line:
            stop = clean_line(full_line.group(1))
            time = full_line.group(2)
            relief = bool(full_line.group(3))
            if pending_fragments:
                stop = clean_line(" ".join(pending_fragments + [stop]))
                flush_pending()
            if stop:
                stop_times.append({
                    "stop": stop,
                    "time": time,
                    "relief": relief,
                })
            i += 1
            continue

        if looks_like_instruction(line):
            flush_pending()
            i += 1
            continue

        short_next = clean_line(section_lines[i + 1]) if i + 1 < len(section_lines) else ""
        time_next = re.match(r"^(\d{1,2}:\d{2})(?:\s+(Relief))?$", short_next)
        if time_next:
            stop_times.append({
                "stop": line,
                "time": time_next.group(1),
                "relief": bool(time_next.group(2)),
            })
            flush_pending()
            i += 2
            continue

        if len(line) <= 40:
            pending_fragments.append(line)
        else:
            flush_pending()
        i += 1

    return stop_times


def format_direction_text(lines):
    cleaned = [clean_line(line) for line in lines if clean_line(line)]
    if not cleaned:
        return None
    text = " ".join(cleaned).strip()
    text = re.sub(r"^Type:\s*\S+\s+", "", text)
    return text.strip() or None


def split_trip_section(section_lines):
    stop_lines = []
    direction_lines = []
    note_lines = []
    in_directions = False
    skipping_note = False
    school_trip_prefix = "THIS IS A SCHOOL TRIP"

    for raw_line in section_lines:
        line = clean_line(raw_line)
        if not line:
            continue

        upper = line.upper()
        if not stop_lines and upper.startswith(school_trip_prefix):
            note_lines.append("This is a school trip")
            line = clean_line(line[len(school_trip_prefix):])
            if not line:
                continue
            upper = line.upper()

        has_time = bool(re.search(r"\d{1,2}:\d{2}", line))
        if not stop_lines and upper.startswith("NOTE:"):
            skipping_note = True
            note_lines.append(line)
            continue
        if skipping_note and not stop_lines:
            if has_time:
                skipping_note = False
            else:
                note_lines.append(line)
                continue

        if not in_directions and looks_like_instruction(line):
            in_directions = True

        if in_directions:
            direction_lines.append(line)
        else:
            stop_lines.append(line)

    return parse_stop_times(stop_lines), format_direction_text(direction_lines), format_direction_text(note_lines)


def is_preamble_metadata_line(line, paddle_id, effective, routes, service_day, bus_type, garage, sign_on):
    stripped = clean_line(line)
    if not stripped:
        return True
    if stripped in {"1", "2", "3", "4"}:
        return True
    if stripped == paddle_id:
        return True
    if stripped == "(Sign-on)":
        return True
    if stripped == sign_on:
        return True
    if stripped == service_day:
        return True
    if stripped == bus_type:
        return True
    if stripped.startswith("Type:"):
        return True
    if stripped == effective:
        return True
    if stripped == f"Effective: {effective}":
        return True
    if stripped.startswith("Trip N°"):
        return True
    if stripped.startswith("Routes:"):
        return True
    if stripped == ", ".join(routes):
        return True
    if re.fullmatch(r"\(TG \d+\)", stripped):
        return True
    return False


def parse_page(text: str, source_meta, page_number: int):
    paddle_match = re.search(r"\n([0-9A-Z]{6})\nEffective:", text)
    routes_match = re.search(r"\nRoutes:\s*([^\n]+)", text)
    effective_match = re.search(r"\nEffective:\s*([^\n]+)", text)
    sign_on_match = re.search(r"\(Sign-on\)\s*\n\s*([0-9:]+)", text)
    garage_match = re.search(r"Trip N°([^\n]+)", text)
    type_match = re.search(r"\nType:\s*([^\n]+)", text)

    if not paddle_match or not routes_match:
        return None

    paddle_id = paddle_match.group(1).strip()
    routes = split_csv_routes(routes_match.group(1).strip())
    effective = effective_match.group(1).strip() if effective_match else None
    sign_on = sign_on_match.group(1).strip() if sign_on_match else None
    garage = clean_line(garage_match.group(1)) if garage_match else None
    bus_type = clean_line(type_match.group(1)) if type_match else None

    lines = [clean_line(line) for line in text.splitlines() if clean_line(line)]
    trips = []
    active_trip = None
    section_lines = []
    preamble_lines = []
    start_directions = None

    def flush_trip():
        nonlocal active_trip, section_lines, start_directions
        if not active_trip:
            section_lines = []
            return

        stop_times, next_directions, notes = split_trip_section(section_lines)
        trip_record = {
            "trip_number": active_trip["trip_number"],
            "route": active_trip["route"],
            "headsign": active_trip["headsign"],
            "start_time": stop_times[0]["time"] if stop_times else None,
            "end_time": stop_times[-1]["time"] if stop_times else None,
            "start_stop": stop_times[0]["stop"] if stop_times else None,
            "end_stop": stop_times[-1]["stop"] if stop_times else None,
            "relief_stop": next((entry["stop"] for entry in stop_times if entry["relief"]), None),
            "relief_time": next((entry["time"] for entry in stop_times if entry["relief"]), None),
            "start_directions": start_directions if not trips else None,
            "next_directions": next_directions,
            "notes": notes,
        }
        trips.append(trip_record)
        start_directions = None
        active_trip = None
        section_lines = []

    i = 0
    while i < len(lines):
        line = lines[i]
        header, consumed = parse_trip_header_at(lines, i, routes)
        if header:
            if not trips and not active_trip:
                start_directions = format_direction_text(preamble_lines)
                preamble_lines = []
            flush_trip()
            active_trip = header
            section_lines = []
            i += consumed
            continue

        if active_trip:
            section_lines.append(line)
        elif not is_preamble_metadata_line(line, paddle_id, effective, routes, source_meta.get("service_header", source_meta["service_day"].capitalize()), bus_type, garage, sign_on):
            preamble_lines.append(line)
        i += 1

    flush_trip()

    if not trips:
        return None

    return {
        "paddle_id": paddle_id,
        "block_label": paddle_id_to_block_label(paddle_id),
        "source_id": source_meta["source_id"],
        "source_label": source_meta["label"],
        "service_day": source_meta["service_day"],
        "category": source_meta["category"],
        "effective": effective,
        "garage": garage,
        "sign_on": sign_on,
        "routes": routes,
        "bus_type": bus_type,
        "page_number": page_number,
        "trips": trips,
    }


def has_paddle_header(text: str) -> bool:
    return bool(re.search(r"\n([0-9A-Z]{6})\nEffective:", text or ""))


def normalize_continuation_page_text(text: str) -> str:
    lines = text.splitlines()
    cleaned = []
    first_non_empty_seen = False

    for line in lines:
        current = line
        if not first_non_empty_seen and clean_line(line):
            current = re.sub(r"^\d+(?=[A-Za-z])", "", current)
            first_non_empty_seen = True
        cleaned.append(current)

    return "\n".join(cleaned)


def build_index():
    runs_by_service = {
        "weekday": {},
        "saturday": {},
        "sunday": {},
        "easter_monday": {},
    }
    source_summaries = {}

    for filename, source_meta in PDF_SOURCES.items():
        pdf_path = PADDLES_DIR / filename
        reader = PdfReader(str(pdf_path))
        parsed_count = 0

        idx = 0
        while idx < len(reader.pages):
            page_text = reader.pages[idx].extract_text() or ""
            if not has_paddle_header(page_text):
                idx += 1
                continue

            combined_text = page_text
            next_idx = idx + 1
            while next_idx < len(reader.pages):
                next_text = reader.pages[next_idx].extract_text() or ""
                if has_paddle_header(next_text):
                    break
                if clean_line(next_text):
                    combined_text += "\n" + normalize_continuation_page_text(next_text)
                next_idx += 1

            parsed = parse_page(combined_text, source_meta, idx + 1)
            if not parsed:
                idx = next_idx
                continue

            runs_by_service[source_meta["service_day"]][parsed["paddle_id"]] = parsed
            parsed_count += 1
            idx = next_idx

        source_summaries[source_meta["source_id"]] = {
            **source_meta,
            "filename": filename,
            "pages": len(reader.pages),
            "parsed_runs": parsed_count,
        }

    return {
        "generated_by": "tools/build_paddle_index.py",
        "sources": source_summaries,
        "service_days": runs_by_service,
    }


def main():
    data = build_index()
    os.makedirs(OUTPUT_PATH.parent, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2)
    weekday_runs = len(data["service_days"]["weekday"])
    saturday_runs = len(data["service_days"]["saturday"])
    sunday_runs = len(data["service_days"]["sunday"])
    easter_monday_runs = len(data["service_days"]["easter_monday"])
    print(f"Wrote {OUTPUT_PATH}")
    print(f"weekday runs: {weekday_runs}")
    print(f"saturday runs: {saturday_runs}")
    print(f"sunday runs: {sunday_runs}")
    print(f"easter monday runs: {easter_monday_runs}")


if __name__ == "__main__":
    main()
