import argparse
import json
import os
import re
from pathlib import Path

from pypdf import PdfReader


ROOT = Path(__file__).resolve().parents[1]
PADDLES_DIR = ROOT / "paddles"
OUTPUT_PATH = ROOT / "data" / "paddles.index.json"
SERVICE_DAYS = (
    "weekday",
    "saturday",
    "sunday",
    "easter_monday",
    "canada_day",
    "civic_holiday",
    "reduced_week",
)

PADDLE_VARIANTS = {
    "winter": {
        "label": "Winter paddles",
        "activation_date": "2026-12-20",
        "sources": {
            "assets/paddles/winter-2026/weekday/Daily JANPDA27 (5-01 to 53-10).pdf": {
                "source_id": "winter_weekday_low", "label": "Weekdays 5-01 to 53-10",
                "service_day": "weekday", "category": "regular",
            },
            "assets/paddles/winter-2026/weekday/Daily JANPDA27 (56-01 to 899-01).pdf": {
                "source_id": "winter_weekday_high", "label": "Weekdays 56-01 to 899-01",
                "service_day": "weekday", "category": "regular",
            },
            "assets/paddles/winter-2026/weekday/JANPDA27 (AM & PM).pdf": {
                "source_id": "winter_weekday_am_pm", "label": "Weekday AM/PM and 900 blocks",
                "service_day": "weekday", "category": "express_900",
            },
            "assets/paddles/winter-2026/saturday/JANPSA27.pdf": {
                "source_id": "winter_saturday", "label": "Saturday paddles",
                "service_day": "saturday", "category": "regular",
            },
            "assets/paddles/winter-2026/sunday/JANPSU27.pdf": {
                "source_id": "winter_sunday", "label": "Sunday paddles",
                "service_day": "sunday", "category": "regular",
            },
            "assets/paddles/winter-2026/Reduced/Reduce week JANPRW27 (5-01 to 56-10).pdf": {
                "source_id": "winter_reduced_low", "label": "Reduced weekdays 5-01 to 56-10",
                "service_day": "reduced_week", "category": "reduced_week", "service_header": "Reduced Week",
            },
            "assets/paddles/winter-2026/Reduced/Reduce weekJANPRW27 (57-01 to 899).pdf": {
                "source_id": "winter_reduced_high", "label": "Reduced weekdays 57-01 to 899",
                "service_day": "reduced_week", "category": "reduced_week", "service_header": "Reduced Week",
            },
        },
    },
    "current": {
        "label": "Current spring paddles",
        "sources": {
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
        },
    },
    "april19": {
        "label": "Spring paddles",
        "activation_date": "2026-04-19",
        "sources": {
            "April19/Weekdays(5-01 to 53-09).pdf": {
                "source_id": "april19_weekday_low",
                "label": "Weekdays 5-01 to 53-09",
                "service_day": "weekday",
                "category": "regular",
            },
            "April19/Weekdays(56-01 to 898-01).pdf": {
                "source_id": "april19_weekday_high",
                "label": "Weekdays 56-01 to 898-01",
                "service_day": "weekday",
                "category": "regular",
            },
            "April19/ExpressAnd900s(AM & PM).pdf": {
                "source_id": "april19_weekday_express_900",
                "label": "Express and 900 series (AM and PM)",
                "service_day": "weekday",
                "category": "express_900",
            },
            "April19/Saturdays.pdf": {
                "source_id": "april19_saturday_main",
                "label": "Saturday paddles",
                "service_day": "saturday",
                "category": "regular",
            },
            "April19/Sundays.pdf": {
                "source_id": "april19_sunday_main",
                "label": "Sunday paddles",
                "service_day": "sunday",
                "category": "regular",
            },
        },
    },
    "june29": {
        "label": "Summer paddles",
        "activation_date": "2026-06-29",
        "sources": {
            "Summer/Summer Daily (5-01 to 53-05).pdf": {
                "source_id": "june29_weekday_low",
                "label": "Weekdays 5-01 to 53-05",
                "service_day": "weekday",
                "category": "regular",
            },
            "Summer/Summer Daily(56-01 to 899-04).pdf": {
                "source_id": "june29_weekday_high",
                "label": "Weekdays 56-01 to 899-04",
                "service_day": "weekday",
                "category": "regular",
            },
            "Summer/Summer (AM PM).pdf": {
                "source_id": "june29_weekday_express_900",
                "label": "Summer AM/PM 900 and 901",
                "service_day": "weekday",
                "category": "express_900",
            },
            "Summer/Summer Saturday .pdf": {
                "source_id": "june29_saturday_main",
                "label": "Saturday paddles",
                "service_day": "saturday",
                "category": "regular",
            },
            "Summer/Summer Sunday.pdf": {
                "source_id": "june29_sunday_main",
                "label": "Sunday paddles",
                "service_day": "sunday",
                "category": "regular",
            },
            "Summer/Canada Day.pdf": {
                "source_id": "june29_canada_day",
                "label": "Canada Day paddles",
                "service_day": "canada_day",
                "category": "holiday",
                "service_header": "Canada Day",
            },
            "Summer/CIVHP26.pdf": {
                "source_id": "june29_civic_holiday",
                "label": "Civic Holiday paddles",
                "service_day": "civic_holiday",
                "category": "holiday",
                "service_header": "Civic Holiday",
            },
        },
    },
    "fall": {
        "label": "Fall paddles",
        "activation_date": "2026-08-31",
        "sources": {
            "Fall Booking/Fall Paddles/FallPaddlesWeekday.pdf": {
                "source_id": "fall_weekday_main",
                "label": "Weekdays",
                "service_day": "weekday",
                "category": "regular",
            },
            "Fall Booking/Fall Paddles/DailyFall(AM PM).pdf": {
                "source_id": "fall_weekday_am_pm",
                "label": "AM/PM paddles",
                "service_day": "weekday",
                "category": "express_900",
            },
            "Fall Booking/Fall Paddles/FallPaddlesSaturday.pdf": {
                "source_id": "fall_saturday_main",
                "label": "Saturday paddles",
                "service_day": "saturday",
                "category": "regular",
            },
            "Fall Booking/Fall Paddles/FallPaddleSunday.pdf": {
                "source_id": "fall_sunday_main",
                "label": "Sunday paddles",
                "service_day": "sunday",
                "category": "regular",
            },
        },
    },
}

INSTRUCTION_PREFIXES = (
    "INTO ",
    "AROUND ",
    "PROCEED ",
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


def parse_trip_header(line: str, routes, expected_trip_number=None):
    text = line.strip()
    if re.search(r"\d{1,2}:\d{2}", text):
        return None
    upper_text = text.upper()
    if any(marker in upper_text for marker in ("EFFECTIVE:", "ROUTES:", "TYPE:", "(TG ", "(SIGN-ON)")):
        return None
    for route in sorted(routes, key=len, reverse=True):
        if not text.endswith(route):
            continue
        rest = text[:-len(route)].strip()
        max_trip_len = min(3, len(rest) - 1)
        lengths = list(range(1, max_trip_len + 1))
        if expected_trip_number is not None:
            lengths.sort(key=lambda size: int(rest[:size]) != expected_trip_number if rest[:size].isdigit() else True)
        for trip_len in lengths:
            trip_digits = rest[:trip_len]
            if not trip_digits.isdigit():
                continue
            trip_number = int(trip_digits)
            if trip_number > 999:
                continue
            headsign = rest[trip_len:].strip()
            if not headsign:
                continue
            if not re.search(r"[A-Za-z]", headsign):
                continue
            if re.match(r"^\d+[^\W\d_]", headsign):
                continue
            return {
                "trip_number": trip_number,
                "headsign": headsign,
                "route": route,
            }
    return None


def parse_trip_header_at(lines, index, routes, expected_trip_number=None):
    if index >= len(lines):
        return None, 0

    max_parts = min(3, len(lines) - index)
    for parts in range(1, max_parts + 1):
        if parts > 1:
            next_line = clean_line(lines[index + parts - 1])
            if parse_trip_header(next_line, routes):
                break
        candidate = clean_line(" ".join(lines[index:index + parts]))
        header = parse_trip_header(candidate, routes, expected_trip_number)
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

        if looks_like_instruction(line):
            flush_pending()
            i += 1
            continue

        if len(line) <= 40:
            pending_fragments.append(line)
        else:
            flush_pending()
        i += 1

    return stop_times


def format_direction_text(lines):
    cleaned = [
        clean_line(line)
        for line in lines
        if clean_line(line) and not re.fullmatch(r"\d+", clean_line(line))
    ]
    if not cleaned:
        return None
    text = " ".join(cleaned).strip()
    text = re.sub(r"^Type:\s*\S+\s+", "", text)
    text = re.sub(r"\b(Garage)(\d{1,2}:\d{2})\b", r"\1 \2", text)
    text = re.sub(r"(\d{1,2}:\d{2})\s+\d+\s*$", r"\1", text)
    return text.strip() or None


def clean_direction_text(text):
    if not text:
        return text
    value = clean_line(text)
    value = re.sub(r"\b(Garage)(\d{1,2}:\d{2})\b", r"\1 \2", value)
    value = re.sub(r"(\d{1,2}:\d{2})\s+\d+\s*$", r"\1", value)
    value = re.sub(r"^\d+(?=\()", "", value)
    return value.strip() or None


def clean_run_direction_text(run):
    if not isinstance(run, dict):
        return run
    for trip in run.get("trips") or []:
        if not isinstance(trip, dict):
            continue
        for key in ("start_directions", "next_directions", "notes"):
            if trip.get(key):
                trip[key] = clean_direction_text(trip[key])
    return run


def split_trip_section(section_lines):
    stop_lines = []
    direction_lines = []
    note_lines = []
    in_directions = False
    skipping_note = False
    school_trip_prefix = "THIS IS A SCHOOL TRIP"

    section_lines = [clean_line(line) for line in section_lines if clean_line(line)]
    for index, raw_line in enumerate(section_lines):
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

        next_line = section_lines[index + 1] if index + 1 < len(section_lines) else ""
        is_timed_stop = has_time or bool(re.fullmatch(r"\d{1,2}:\d{2}(?:\s+Relief)?", next_line))
        if not in_directions and not is_timed_stop and looks_like_instruction(line):
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
        expected_trip = (active_trip or (trips[-1] if trips else {})).get("trip_number", 0) + 1
        header, consumed = parse_trip_header_at(lines, i, routes, expected_trip)
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

    parse_warnings = []
    if source_meta.get("variant_id") == "winter":
        numbers = [trip["trip_number"] for trip in trips]
        if numbers != list(range(1, len(numbers) + 1)):
            previous, current = next((previous, current) for previous, current in zip([0] + numbers, numbers) if current != previous + 1)
            parse_warnings.append(f"Trip numbering needs manual review: the sequence jumps from #{previous} to #{current}. All source pages have been included.")

    return {
        "paddle_id": paddle_id,
        "block_label": paddle_id_to_block_label(paddle_id),
        "source_id": source_meta["source_id"],
        "source_label": source_meta["label"],
        "variant_id": source_meta.get("variant_id") or "current",
        "variant_label": source_meta.get("variant_label") or "Current spring paddles",
        "service_day": source_meta["service_day"],
        "category": source_meta["category"],
        "effective": effective,
        "garage": garage,
        "sign_on": sign_on,
        "routes": routes,
        "bus_type": bus_type,
        "page_number": page_number,
        "trips": trips,
        **({"parse_warnings": parse_warnings} if parse_warnings else {}),
    }


def has_paddle_header(text: str) -> bool:
    return bool(re.search(r"\n([0-9A-Z]{6})\nEffective:", text or ""))


def normalize_continuation_page_text(text: str, page_number: int) -> str:
    lines = text.splitlines()
    cleaned = []
    first_non_empty_seen = False

    for line in lines:
        current = line
        if not first_non_empty_seen and clean_line(line):
            first_non_empty_seen = True
            prefix = str(page_number)
            if current.lstrip().startswith(prefix):
                current = current.lstrip()[len(prefix):].lstrip()
                if not current:
                    continue
        cleaned.append(current)

    return "\n".join(cleaned)


def iter_paddle_documents(reader):
    first_page = None
    parts = []
    for page_number, page in enumerate(reader.pages, 1):
        text = page.extract_text() or ""
        if has_paddle_header(text):
            if parts:
                yield first_page, page_number - 1, "\n".join(parts)
            first_page = page_number
            parts = [text]
        elif parts and clean_line(text) and not re.fullmatch(r"\d+", clean_line(text)):
            parts.append(normalize_continuation_page_text(text, page_number - first_page + 1))
    if parts:
        yield first_page, len(reader.pages), "\n".join(parts)


def resolve_paddle_source_path(filename: str) -> Path:
    paddle_path = PADDLES_DIR / filename
    if paddle_path.exists():
        return paddle_path
    return ROOT / filename


def build_index(selected_variants=None, output_path=OUTPUT_PATH):
    existing_index = {}
    if output_path.exists():
        with open(output_path, "r", encoding="utf-8") as fh:
            existing_index = json.load(fh)

    runs_by_variant = json.loads(json.dumps(existing_index.get("variants", {})))
    source_summaries = json.loads(json.dumps(existing_index.get("sources", {})))

    for variant_id, variant_meta in PADDLE_VARIANTS.items():
        if selected_variants and variant_id not in selected_variants:
            continue
        available_sources = [
            filename
            for filename in variant_meta["sources"]
            if resolve_paddle_source_path(filename).exists()
        ]
        if available_sources or variant_id not in runs_by_variant:
            runs_by_variant[variant_id] = {
                "label": variant_meta["label"],
                "activation_date": variant_meta.get("activation_date"),
                "service_days": {service_day: {} for service_day in SERVICE_DAYS},
            }
            source_summaries = {
                source_id: summary
                for source_id, summary in source_summaries.items()
                if summary.get("variant_id") != variant_id
            }
        else:
            runs_by_variant[variant_id]["label"] = variant_meta["label"]
            runs_by_variant[variant_id]["activation_date"] = variant_meta.get("activation_date")
            runs_by_variant[variant_id].setdefault("service_days", {})
            for service_day in SERVICE_DAYS:
                runs_by_variant[variant_id]["service_days"].setdefault(service_day, {})

        for filename, base_source_meta in variant_meta["sources"].items():
            source_meta = {
                **base_source_meta,
                "variant_id": variant_id,
                "variant_label": variant_meta["label"],
            }
            pdf_path = resolve_paddle_source_path(filename)
            if not pdf_path.exists():
                continue

            reader = PdfReader(str(pdf_path))
            parsed_count = 0

            for first_page, last_page, combined_text in iter_paddle_documents(reader):
                parsed = parse_page(combined_text, source_meta, first_page)
                if not parsed:
                    continue

                parsed["source_page_end"] = last_page
                parsed["source_page_count"] = last_page - first_page + 1

                service_day = source_meta["service_day"]
                if variant_id == "winter" and parsed["paddle_id"] in runs_by_variant[variant_id]["service_days"][service_day]:
                    raise ValueError(f"Duplicate winter paddle {parsed['paddle_id']} for {service_day}: {filename}")
                runs_by_variant[variant_id]["service_days"][service_day][parsed["paddle_id"]] = parsed
                parsed_count += 1

            source_summaries[source_meta["source_id"]] = {
                **source_meta,
                "filename": filename,
                "pages": len(reader.pages),
                "parsed_runs": parsed_count,
            }
            print(f"{source_meta['source_id']}: {parsed_count} paddles / {len(reader.pages)} pages", flush=True)
            if variant_id == "winter" and parsed_count == 0:
                raise ValueError(f"No paddles parsed from {filename}")

    latest_default_variant_by_service_day = {
        "weekday": "june29",
        "saturday": "june29",
        "sunday": "june29",
        "easter_monday": "current",
        "canada_day": "june29",
        "civic_holiday": "june29",
    }
    runs_by_service = {service_day: {} for service_day in SERVICE_DAYS}
    for service_day, variant_id in latest_default_variant_by_service_day.items():
        variant = runs_by_variant.get(variant_id) or {}
        runs_by_service[service_day] = (variant.get("service_days") or {}).get(service_day, {}) or {}

    for variant in runs_by_variant.values():
        for runs in (variant.get("service_days") or {}).values():
            for run in (runs or {}).values():
                clean_run_direction_text(run)

    for runs in runs_by_service.values():
        for run in (runs or {}).values():
            clean_run_direction_text(run)

    return {
        "generated_by": "tools/build_paddle_index.py",
        "sources": source_summaries,
        "service_days": runs_by_service,
        "variants": runs_by_variant,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--variant", choices=PADDLE_VARIANTS, action="append")
    parser.add_argument("--output", type=Path, default=OUTPUT_PATH)
    args = parser.parse_args()
    data = build_index(args.variant, args.output)
    os.makedirs(args.output.parent, exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2)
    weekday_runs = len(data["service_days"]["weekday"])
    saturday_runs = len(data["service_days"]["saturday"])
    sunday_runs = len(data["service_days"]["sunday"])
    easter_monday_runs = len(data["service_days"]["easter_monday"])
    canada_day_runs = len(data["service_days"]["canada_day"])
    civic_holiday_runs = len(data["service_days"]["civic_holiday"])
    print(f"Wrote {args.output}")
    print(f"weekday runs: {weekday_runs}")
    print(f"saturday runs: {saturday_runs}")
    print(f"sunday runs: {sunday_runs}")
    print(f"easter monday runs: {easter_monday_runs}")
    print(f"canada day runs: {canada_day_runs}")
    print(f"civic holiday runs: {civic_holiday_runs}")


if __name__ == "__main__":
    main()
