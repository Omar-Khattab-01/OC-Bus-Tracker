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
    match = re.match(r"^(\d+)(.+)$", text)
    if not match:
        return None

    trip_number = int(match.group(1))
    rest = match.group(2).strip()
    for route in sorted(routes, key=len, reverse=True):
      if rest.endswith(route):
        headsign = rest[:-len(route)].strip()
        if headsign:
          return {
              "trip_number": trip_number,
              "headsign": headsign,
              "route": route,
          }
    return None


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

    def flush_trip():
        nonlocal active_trip, section_lines
        if not active_trip:
            section_lines = []
            return

        stop_times = parse_stop_times(section_lines)
        trip_record = {
            "trip_number": active_trip["trip_number"],
            "route": active_trip["route"],
            "headsign": active_trip["headsign"],
            "start_time": stop_times[0]["time"] if stop_times else None,
            "end_time": stop_times[-1]["time"] if stop_times else None,
            "start_stop": stop_times[0]["stop"] if stop_times else None,
            "end_stop": stop_times[-1]["stop"] if stop_times else None,
            "relief_stop": next((entry["stop"] for entry in stop_times if entry["relief"]), None),
        }
        trips.append(trip_record)
        active_trip = None
        section_lines = []

    for line in lines:
        header = parse_trip_header(line, routes)
        if header:
            flush_trip()
            active_trip = header
            section_lines = []
            continue

        if active_trip:
            section_lines.append(line)

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


def build_index():
    runs_by_service = {
        "weekday": {},
        "saturday": {},
        "sunday": {},
    }
    source_summaries = {}

    for filename, source_meta in PDF_SOURCES.items():
        pdf_path = PADDLES_DIR / filename
        reader = PdfReader(str(pdf_path))
        parsed_count = 0

        for idx, page in enumerate(reader.pages):
            page_text = page.extract_text() or ""
            if "Trip N°" not in page_text or "Effective:" not in page_text:
                continue

            parsed = parse_page(page_text, source_meta, idx + 1)
            if not parsed:
                continue

            runs_by_service[source_meta["service_day"]][parsed["paddle_id"]] = parsed
            parsed_count += 1

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
    print(f"Wrote {OUTPUT_PATH}")
    print(f"weekday runs: {weekday_runs}")
    print(f"saturday runs: {saturday_runs}")
    print(f"sunday runs: {sunday_runs}")


if __name__ == "__main__":
    main()
