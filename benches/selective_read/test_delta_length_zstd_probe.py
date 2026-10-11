import unittest

from delta_length_zstd_probe import raw_headers


class RawHeaderProbeTests(unittest.TestCase):
    def test_headers_are_discovered_from_responses_within_the_page(self):
        value = bytes(range(256)) * 128
        frame = b"\x28\xb5\x2f\xfd\xa0" + len(value).to_bytes(4, "little")
        expected = []
        for start in range(0, len(value), 10001):
            part = value[start:start + 10001]
            expected.append({"header_offset": len(frame), "kind": "raw",
                "stored_bytes": len(part), "decoded_bytes": len(part)})
            frame += ((len(part) << 3) | int(start + len(part) == len(value))).to_bytes(3, "little") + part
        page = {"page_bytes": len(frame), "body_bytes": len(value), "zstd": {"page_header_bytes": 0}}
        requests = []
        def fetch(start, end):
            self.assertLessEqual(end, len(frame))
            requests.append((start, end))
            return frame[start:end]
        self.assertEqual(raw_headers(page, fetch), expected)
        self.assertEqual(requests, [(0, 8192)] + [(p["header_offset"], p["header_offset"] + 3) for p in expected[1:]])
        for changed in (dict(page, body_bytes=len(value) + 1), dict(page, page_bytes=len(frame) - 1)):
            with self.assertRaises(AssertionError):
                raw_headers(changed, fetch)
        altered = bytearray(frame)
        altered[expected[1]["header_offset"]] |= 4
        with self.assertRaisesRegex(AssertionError, "raw block"):
            raw_headers(page, lambda start, end: altered[start:end])
        altered = bytearray(frame)
        altered[4] |= 4
        with self.assertRaisesRegex(AssertionError, "checksum"):
            raw_headers(page, lambda start, end: altered[start:end])


if __name__ == "__main__":
    unittest.main()
