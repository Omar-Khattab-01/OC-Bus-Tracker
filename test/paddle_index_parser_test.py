import unittest
from types import SimpleNamespace
from tools.build_paddle_index import (iter_paddle_documents, normalize_continuation_page_text,
                                      parse_page, parse_trip_header, split_trip_section)


class PaddleHeaderTest(unittest.TestCase):
    def test_numeric_destinations_keep_the_expected_trip_number(self):
        header = parse_trip_header("113940 Innes Rd31", ["31"], 11)
        self.assertEqual(header, {"trip_number": 11, "headsign": "3940 Innes Rd", "route": "31"})
        header = parse_trip_header("18925 Belfast895", ["895"], 18)
        self.assertEqual(header["headsign"], "925 Belfast")
        self.assertEqual(header["trip_number"], 18)

    def test_accented_destinations_do_not_consume_trip_digits(self):
        header = parse_trip_header("11\u00c9S Louis Riel628", ["628"], 11)
        self.assertEqual(header["trip_number"], 11)
        self.assertEqual(header["headsign"], "\u00c9S Louis Riel")

    def test_stop_times_are_never_trip_headers(self):
        for text in ["925 Belfast 1:19", "2Ogilvie/Lassiter19:31"]:
            self.assertIsNone(parse_trip_header(text, ["19", "31"], 9))

    def test_timed_roadway_stops_are_not_deadhead_directions(self):
        for lines in [["Hurdman Station D 9:40", "Roadway at Belfast 9:46", "Blair Station C 10:09", "(L) ROADWAY, (R) BLAIR."],
                      ["Hurdman Station D 9:40", "Roadway at Belfast", "9:46", "Blair Station C 10:09", "(L) ROADWAY, (R) BLAIR."]]:
            times, directions, _ = split_trip_section(lines)
            self.assertEqual(times[-1]["time"], "10:09")
            self.assertEqual(times[1]["stop"], "Roadway at Belfast")
            self.assertEqual(directions, "(L) ROADWAY, (R) BLAIR.")

    def test_garage_arrival_after_proceed_is_deadhead_not_service(self):
        for instruction in ["PROCEED TO DEPOT.", "INTO GARAGE.", "AROUND PARKING LOT INTO DEPOT."]:
            times, directions, _ = split_trip_section([
                "St Laurent Depot 1:15", instruction, "St L South Garage 1:21"])
            self.assertEqual(times[-1]["time"], "1:15")
            self.assertIn("1:21", directions)

    def test_continuation_prefix_preserves_trip_and_stop_digits(self):
        self.assertEqual(normalize_continuation_page_text("212Hurdman Station90", 2), "12Hurdman Station90")
        self.assertEqual(normalize_continuation_page_text("2925 Belfast 1:19", 2), "925 Belfast 1:19")
        self.assertEqual(normalize_continuation_page_text("2\nGreenboro Station B 20:40", 2), "Greenboro Station B 20:40")

    def test_three_page_paddle_keeps_continuation_before_next_paddle(self):
        first = "1\nTrip N°Industrial Garage\n(Sign-on)\n4:30\n005001\nEffective: December 21, 2026\nRoutes: 17\nWeekday\nType: ELNI\n1Wateridge17\nFirst Stop 5:00"
        fourth = first.replace("005001", "005002")
        texts = [first, "2\nLast Stop 5:20", "32Wateridge17\nFirst Stop 5:30\nLast Stop 5:50", fourth]
        reader = SimpleNamespace(pages=[SimpleNamespace(extract_text=lambda text=text: text) for text in texts])
        documents = list(iter_paddle_documents(reader))
        self.assertEqual([(first, last) for first, last, _ in documents], [(1, 3), (4, 4)])
        parsed = parse_page(documents[0][2], {
            "source_id": "winter_test", "label": "Weekday", "service_day": "weekday",
            "category": "regular", "variant_id": "winter"}, 1)
        self.assertEqual([trip["trip_number"] for trip in parsed["trips"]], [1, 2])
        self.assertEqual(parsed["trips"][0]["end_time"], "5:20")
        self.assertEqual(parsed["trips"][0]["end_stop"], "Last Stop")
        self.assertEqual(parsed["trips"][1]["end_time"], "5:50")
        self.assertNotIn("parse_warnings", parsed)


if __name__ == "__main__":
    unittest.main()
