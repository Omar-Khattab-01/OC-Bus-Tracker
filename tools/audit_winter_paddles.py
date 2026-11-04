"""Compare imported trips and multi-page boundaries to PDF coordinates."""

import argparse
import json
import re
import subprocess
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
NS = {"x": "http://www.w3.org/1999/xhtml"}
COLUMN_LEFT = 23.4
COLUMN_WIDTH = 247.35


def extract_pdf_runs(pdf_path):
    runs = {}
    page_spans = {}
    current = None
    active_trip = None
    page_number = 0
    with tempfile.TemporaryDirectory() as tmp:
        output = Path(tmp) / "coordinates.html"
        subprocess.run(["pdftotext", "-bbox-layout", str(pdf_path), str(output)], check=True)
        for _, page in ET.iterparse(output, events=("end",)):
            if page.tag != f"{{{NS['x']}}}page":
                continue
            page_number += 1
            words = [
                {"text": word.text or "", "x": float(word.attrib["xMin"]), "y": float(word.attrib["yMin"]),
                 "height": float(word.attrib["yMax"]) - float(word.attrib["yMin"])}
                for word in page.findall(".//x:word", NS)
            ]
            identifiers = [word["text"] for word in words
                           if re.fullmatch(r"\d{6}", word["text"]) and word["x"] < 80 and word["y"] < 60]
            if identifiers:
                current = identifiers[0]
                active_trip = None
                if current in runs:
                    raise ValueError(f"Repeated paddle header: {current}")
                runs[current] = []
                page_spans[current] = []
            if current is None:
                page.clear()
                continue
            page_spans[current].append(page_number)
            for column in range(4):
                left = COLUMN_LEFT + column * COLUMN_WIDTH
                column_words = [word for word in words if left - 1 <= word["x"] < left + COLUMN_WIDTH - 1]
                headers = []
                for route in column_words:
                    if abs(route["x"] - left) > 1 or not re.fullmatch(r"\d{1,3}", route["text"]):
                        continue
                    sequence = [word for word in column_words
                                if left + 200 <= word["x"] < left + 225
                                and abs(word["y"] - route["y"]) < 1.2
                                and re.fullmatch(r"\d{1,3}", word["text"])]
                    if len(sequence) == 1:
                        headers.append({"trip_number": int(sequence[0]["text"]), "route": route["text"], "y": route["y"]})
                times = [word for word in column_words if left - 1 <= word["x"] < left + 35
                         and re.fullmatch(r"\d{1,2}:\d{2}", word["text"])]
                instructions = [word for word in column_words
                                if (abs(word["x"] - (left + 39)) < 2 and word["height"] < 8)
                                or (abs(word["x"] - (left - 1)) < 2 and word["text"] == 'Conduct')]
                events = ([(header["y"], 0, header) for header in headers]
                          + [(word["y"], 1, word) for word in times]
                          + [(word["y"], 2, word) for word in instructions])
                for _, kind, item in sorted(events, key=lambda event: (event[0], event[1])):
                    if kind == 0:
                        active_trip = {"trip_number": item["trip_number"], "route": item["route"],
                                       "start_time": None, "end_time": None, "end_page": page_number, "directions_started": False}
                        runs[current].append(active_trip)
                    elif active_trip is not None and kind == 2:
                        if active_trip["start_time"] is not None:
                            active_trip["directions_started"] = True
                    elif active_trip is not None and not active_trip["directions_started"]:
                        if active_trip["start_time"] is None:
                            active_trip["start_time"] = item["text"]
                        active_trip["end_time"] = item["text"]
                        active_trip["end_page"] = page_number
            page.clear()
    return runs, page_spans


def audit(index):
    mismatches = []
    source_issues = []
    totals = {"sources": 0, "pages": 0, "paddles": 0, "multi_page_paddles": 0,
              "continuation_pages": 0, "trip_headers": 0, "first_departures_checked": 0,
              "trip_arrivals_checked": 0}
    for source_id, source in index["sources"].items():
        pdf_runs, page_spans = extract_pdf_runs(ROOT / source["filename"])
        imported = {key: value for key, value in index["variants"]["winter"]["service_days"][source["service_day"]].items()
                    if value["source_id"] == source_id}
        totals["sources"] += 1
        totals["pages"] += sum(len(pages) for pages in page_spans.values())
        totals["multi_page_paddles"] += sum(len(pages) > 1 for pages in page_spans.values())
        totals["continuation_pages"] += sum(len(pages) - 1 for pages in page_spans.values())
        totals["paddles"] += len(pdf_runs)
        for paddle_id in sorted(set(pdf_runs) | set(imported)):
            expected = pdf_runs.get(paddle_id)
            run = imported.get(paddle_id)
            if expected is None or run is None:
                mismatches.append({"source": source_id, "paddle": paddle_id, "issue": "Paddle absent from PDF or index"})
                continue
            actual = run["trips"]
            if run["page_number"] != page_spans[paddle_id][0]:
                mismatches.append({"source": source_id, "paddle": paddle_id, "issue": "Incorrect first source page"})
            if run.get("source_page_end") != page_spans[paddle_id][-1] or run.get("source_page_count") != len(page_spans[paddle_id]):
                mismatches.append({"source": source_id, "paddle": paddle_id, "issue": "Incorrect continuation-page coverage",
                                   "expected_pages": page_spans[paddle_id]})
            totals["trip_headers"] += len(expected)
            expected_keys = [(trip["trip_number"], trip["route"]) for trip in expected]
            actual_keys = [(trip["trip_number"], trip["route"]) for trip in actual]
            if expected_keys != actual_keys:
                mismatches.append({"source": source_id, "paddle": paddle_id, "expected": expected_keys, "actual": actual_keys})
                continue
            for pdf_trip, parsed_trip in zip(expected, actual):
                if pdf_trip["start_time"]:
                    totals["first_departures_checked"] += 1
                    if pdf_trip["start_time"] != parsed_trip["start_time"]:
                        mismatches.append({"source": source_id, "paddle": paddle_id, "trip": pdf_trip["trip_number"],
                                           "expected_start": pdf_trip["start_time"], "actual_start": parsed_trip["start_time"]})
                else:
                    mismatches.append({"source": source_id, "paddle": paddle_id, "trip": pdf_trip["trip_number"],
                                       "issue": "Could not independently locate the first departure time"})
                if pdf_trip["end_time"]:
                    totals["trip_arrivals_checked"] += 1
                    if pdf_trip["end_time"] != parsed_trip["end_time"]:
                        mismatches.append({"source": source_id, "paddle": paddle_id, "trip": pdf_trip["trip_number"],
                                           "expected_end": pdf_trip["end_time"], "actual_end": parsed_trip["end_time"],
                                           "end_page": pdf_trip["end_page"]})
            if run.get("parse_warnings"):
                source_issues.append({"source": source_id, "paddle": paddle_id, "warnings": run["parse_warnings"]})
        print(f"Audited {source_id}: {len(pdf_runs)} paddles", flush=True)
    return {**totals, "mismatches": mismatches, "source_issues": source_issues}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    with (ROOT / "data/winter_paddles.index.json").open() as source:
        report = audit(json.load(source))
    if args.report:
        args.report.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))
    raise SystemExit(bool(report["mismatches"]))


if __name__ == "__main__":
    main()
