"""Capture exact Arrow timestamp comparisons with Python integers and zoneinfo.

Spark only represents microseconds. Verify its local-time conversion separately,
then retain sub-microsecond remainders with integer arithmetic in the fixture.
Use the Spark reference environment documented in README.md.
"""
import datetime as dt
import json
import os
from pathlib import Path
import sys
import time
from zoneinfo import ZoneInfo

EPOCH = dt.datetime(1970, 1, 1)
SCALE = {"date32": 86400_000_000_000, "date64": 1_000_000,
         "s": 1_000_000_000, "ms": 1_000_000, "us": 1000, "ns": 1}
SCENARIOS = [
    ("UTC", "2026-01-02 00:00:00"),
    ("UTC", "1969-12-31 23:59:59.999999"),
    ("America/Phoenix", "2026-01-02 03:04:05.123456"),
    ("+05:45", "2026-01-02 00:00:00"),
    ("-07:00", "2026-01-02 00:00:00"),
    ("America/New_York", "2026-03-08 02:30:00.123456"),
    ("America/New_York", "2026-11-01 01:30:00.123456"),
    ("America/New_York", "1969-10-26 01:59:59.999999"),
    ("Pacific/Apia", "2011-12-30 00:00:00"),
]


def epoch_microseconds(value):
    elapsed = value - EPOCH
    return (elapsed.days * 86400 + elapsed.seconds) * 1_000_000 + elapsed.microseconds


def session_zone(name):
    if name[0] in "+-":
        sign = 1 if name[0] == "+" else -1
        hours, minutes = map(int, name[1:].split(":"))
        return dt.timezone(sign * dt.timedelta(hours=hours, minutes=minutes))
    return ZoneInfo(name)


def local_to_utc_ns(nanoseconds, timezone):
    micros, remainder = divmod(nanoseconds, 1000)
    local = EPOCH + dt.timedelta(microseconds=micros)
    # fold=0 selects the earlier overlap and the pre-transition offset for gaps.
    instant = local.replace(tzinfo=session_zone(timezone), fold=0).astimezone(dt.timezone.utc)
    return epoch_microseconds(instant.replace(tzinfo=None)) * 1000 + remainder


def comparison_values(input_type, values, timezone, has_instant):
    unit, annotation = input_type
    result = []
    for value in values:
        if value is None:
            result.append(None)
            continue
        nanoseconds = value * SCALE[unit]
        if has_instant and annotation is None:
            nanoseconds = local_to_utc_ns(nanoseconds, timezone)
        result.append(nanoseconds)
    return result


def capture_cases():
    cases = []

    def add(name, left_type, right_type, pairs, timezone="UTC"):
        pairs = [*pairs, (None, pairs[0][1]), (pairs[0][0], None), (None, None)]
        left, right = map(list, zip(*pairs))
        has_instant = left_type[1] is not None or right_type[1] is not None
        first = comparison_values(left_type, left, timezone, has_instant)
        second = comparison_values(right_type, right, timezone, has_instant)
        orders = [None if a is None or b is None else (a > b) - (a < b)
                  for a, b in zip(first, second)]
        joins = [[i, j] for i, a in enumerate(first) for j, b in enumerate(second)
                 if a is not None and b is not None and a == b]
        cases.append(dict(id=name, timezone=timezone, left_type=left_type, right_type=right_type,
                          left=left, right=right, order=orders, joins=joins))

    for unit in ["ns", "us", "ms", "s"]:
        scale = SCALE[unit]
        pairs = [(scale + 1, 1), (scale, 1), (-1, 0), (-scale - 1, -1)]
        for left_zone, right_zone in [(None, None), ("UTC", "UTC"), ("UTC", "America/New_York")]:
            add(f"precision_{unit}_{left_zone}_{right_zone}", ("ns", left_zone),
                (unit, right_zone), pairs, "America/Phoenix")

    for index, (timezone, text) in enumerate(SCENARIOS):
        local_ns = epoch_microseconds(dt.datetime.fromisoformat(text)) * 1000
        instant_ns = local_to_utc_ns(local_ns, timezone)
        for unit in ["us", "ns"]:
            scale = SCALE[unit]
            add(f"local_{index}_{unit}", ("ns", None), (unit, "UTC"),
                [(local_ns, instant_ns // scale),
                 (local_ns + 999, (instant_ns + 1000) // scale),
                 (local_ns - 1, instant_ns // scale)], timezone)
        midnight = dt.datetime.fromisoformat(text[:10])
        days = (midnight - EPOCH).days
        midnight_ns = local_to_utc_ns(days * SCALE["date32"], timezone)
        for date_unit in ["date32", "date64"]:
            date_ticks = days * (1 if date_unit == "date32" else 86400_000)
            add(f"date_{index}_{date_unit}", (date_unit, None), ("ns", "UTC"),
                [(date_ticks, midnight_ns), (date_ticks, midnight_ns + 1),
                 (date_ticks, midnight_ns - 1)], timezone)

    minimum, maximum = -(2**63), 2**63 - 1
    for unit in ["us", "ms", "s", "date32", "date64"]:
        bounds = (-(2**31), 2**31 - 1) if unit == "date32" else (minimum, maximum)
        for annotation in ([None] if unit.startswith("date") else [None, "UTC"]):
            add(f"bounds_{unit}_{annotation}", ("ns", annotation), (unit, annotation),
                [(minimum, bounds[0]), (maximum, bounds[1]), (0, 0)])
    for timezone in ["America/Phoenix", "+05:45"]:
        # The converted instant can exceed the representable i64 nanosecond range.
        add(f"local_bounds_{timezone}", ("ns", None), ("us", "UTC"),
            [(minimum, minimum // 1000), (maximum, maximum // 1000)], timezone)
    assert len({case["id"] for case in cases}) == len(cases)
    return cases


def verify_spark():
    from pyspark.sql import SparkSession
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["TZ"] = "UTC"
    if hasattr(time, "tzset"):
        time.tzset()
    spark = (SparkSession.builder.master("local[1]").appName("dar-timestamp-precision")
             .config("spark.ui.enabled", "false").getOrCreate())
    observations = []
    try:
        assert spark.version == "4.2.0", spark.version
        spark.sparkContext.setLogLevel("ERROR")
        for timezone, text in SCENARIOS:
            spark.conf.set("spark.sql.session.timeZone", timezone)
            micros = epoch_microseconds(dt.datetime.fromisoformat(text))
            expected = local_to_utc_ns(micros * 1000, timezone) // 1000
            for ansi in [True, False]:
                spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
                sql = f"SELECT unix_micros(CAST(CAST('{text}' AS TIMESTAMP_NTZ) AS TIMESTAMP))"
                actual = spark.sql(sql).first()[0]
                assert actual == expected, (timezone, text, expected, actual)
                observations.append(dict(timezone=timezone, local=text, ansi=ansi, utc_us=actual))
        return {"spark_version": spark.version, "timezone_checks": observations}
    finally:
        spark.stop()


if __name__ == "__main__":
    cases = capture_cases()
    metadata = {"oracle": "Python exact integers and zoneinfo; no floating point timestamps",
                **verify_spark()}
    Path(__file__).with_name("timestamp-precision.jsonl").write_text(
        "\n".join(json.dumps(record, separators=(",", ":")) for record in [metadata, *cases]) + "\n")
    print(json.dumps({"cases": len(cases), "spark_timezone_checks": len(metadata["timezone_checks"])}))
