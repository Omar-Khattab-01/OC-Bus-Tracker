import json
from pathlib import Path

from pypdf import PdfReader


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_PATH = ROOT / "data" / "fall_pdf_search_index.json"

DOCUMENTS = [
    {
        "id": "fall_weekday",
        "title": "Fall Paddle Weekday",
        "kind": "Paddle",
        "path": "Fall Booking/Fall Paddles/FallPaddlesWeekday.pdf",
        "url": "/fall-paddles/files/FallPaddlesWeekday.pdf",
    },
    {
        "id": "fall_am_pm",
        "title": "Fall Paddle AM/PM",
        "kind": "Paddle",
        "path": "Fall Booking/Fall Paddles/DailyFall(AM PM).pdf",
        "url": "/fall-paddles/files/DailyFall%28AM%20PM%29.pdf",
    },
    {
        "id": "fall_saturday",
        "title": "Fall Paddle Saturday",
        "kind": "Paddle",
        "path": "Fall Booking/Fall Paddles/FallPaddlesSaturday.pdf",
        "url": "/fall-paddles/files/FallPaddlesSaturday.pdf",
    },
    {
        "id": "fall_sunday",
        "title": "Fall Paddle Sunday",
        "kind": "Paddle",
        "path": "Fall Booking/Fall Paddles/FallPaddleSunday.pdf",
        "url": "/fall-paddles/files/FallPaddleSunday.pdf",
    },
    {
        "id": "fall_headways_daily",
        "title": "Fall Headways Daily",
        "kind": "Headways",
        "path": "Fall Booking/Headways/DailyFallHeadways.pdf",
        "url": "/fall-paddles/headways/DailyFallHeadways.pdf",
    },
    {
        "id": "fall_headways_saturday",
        "title": "Fall Headways Saturday",
        "kind": "Headways",
        "path": "Fall Booking/Headways/FallHeadwaysSaturday.pdf",
        "url": "/fall-paddles/headways/FallHeadwaysSaturday.pdf",
    },
    {
        "id": "fall_headways_sunday",
        "title": "Fall Headways Sunday",
        "kind": "Headways",
        "path": "Fall Booking/Headways/FallHeadwaysSundays.pdf",
        "url": "/fall-paddles/headways/FallHeadwaysSundays.pdf",
    },
]


def extract_pages(pdf_path):
    reader = PdfReader(str(pdf_path))
    pages = []
    for index, page in enumerate(reader.pages, start=1):
        text = page.extract_text() or ""
        pages.append({"page": index, "text": " ".join(text.split())})
    return pages


def main():
    docs = []
    for doc in DOCUMENTS:
        pdf_path = ROOT / doc["path"]
        if not pdf_path.exists():
            raise FileNotFoundError(pdf_path)
        docs.append({**doc, "pages": extract_pages(pdf_path)})

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps({"documents": docs}, ensure_ascii=False), encoding="utf-8")
    print(f"Wrote {OUTPUT_PATH} with {len(docs)} documents")


if __name__ == "__main__":
    main()
