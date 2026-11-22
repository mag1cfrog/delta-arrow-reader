"""Run each public SQL shape in a fresh process and record its terminal outcome."""

from pathlib import Path
import json
import subprocess
import sys

MIN = -8334601228800000000
MAX = 8210266876799999999

CASES = [
    ("classic_positive_at_max", MAX, "UTC", "convert_timezone('UTC','+18:00',ntz)"),
    ("classic_negative_at_min", MIN, "UTC", "convert_timezone('UTC','-18:00',ntz)"),
    ("classic_named_at_min", MIN, "UTC", "convert_timezone('UTC','America/Los_Angeles',ntz)"),
    ("classic_literal_at_max", MAX, "UTC", "convert_timezone('UTC','+18:00',timestamp_ntz '+262142-12-31 23:59:59.999999')"),
    ("classic_unsupported_alias", MAX, "UTC", "convert_timezone('UTC','ROC',ntz)"),
    ("from_positive_at_max", MAX, "UTC", "from_utc_timestamp(ltz,'+18:00')"),
    ("from_negative_at_min", MIN, "UTC", "from_utc_timestamp(ltz,'-18:00')"),
    ("from_named_at_min", MIN, "UTC", "from_utc_timestamp(ltz,'America/Los_Angeles')"),
    ("from_unsupported_alias", MAX, "UTC", "from_utc_timestamp(ltz,'ROC')"),
    ("from_ntz_underflow", MIN, "+18:00", "from_utc_timestamp(ntz,'UTC')"),
    ("from_ntz_overflow", MAX, "-18:00", "from_utc_timestamp(ntz,'UTC')"),
    ("to_ntz_underflow", MIN, "+18:00", "to_utc_timestamp(ntz,'UTC')"),
    ("to_ntz_overflow", MAX, "-18:00", "to_utc_timestamp(ntz,'UTC')"),
    ("to_ltz_recoverable", MIN, "UTC", "to_utc_timestamp(ltz,'+18:00')"),
    ("to_ltz_inside", MIN, "UTC", "to_utc_timestamp(ltz,'-18:00')"),
    ("make_with_zone_at_min", MIN, "-18:00", "make_timestamp(d,tod,'UTC')"),
    ("make_with_zone_at_max", MAX, "+18:00", "make_timestamp(d,tod,'UTC')"),
    ("try_make_with_zone_at_min", MIN, "-18:00", "try_make_timestamp(d,tod,'UTC')"),
    ("try_make_with_zone_at_max", MAX, "+18:00", "try_make_timestamp(d,tod,'UTC')"),
    ("make_without_zone_underflow", MIN, "+18:00", "make_timestamp(d,tod)"),
    ("try_make_without_zone_overflow", MAX, "-18:00", "try_make_timestamp(d,tod)"),
    ("make_named_session_at_min", MIN, "Europe/Paris", "make_timestamp(d,tod)"),
    ("try_make_named_session_at_max", MAX, "America/Los_Angeles", "try_make_timestamp(d,tod)"),
    ("make_unsupported_session_alias", MIN, "ROC", "make_timestamp(d,tod)"),
    ("make_ltz_alias_at_adak_min", MIN, "America/Adak", "make_timestamp_ltz(d,tod)"),
    ("try_make_ltz_alias_at_adak_max", MAX, "America/Adak", "try_make_timestamp_ltz(d,tod)"),
    ("classic_preexisting_null", MAX + 1, "UTC", "convert_timezone('UTC','+18:00',ntz)"),
    ("from_preexisting_error", 2**63 - 1, "UTC", "from_utc_timestamp(ltz,'UTC')"),
]


def observe(binary, name, micros, zone, ansi, expression):
    result = subprocess.run(
        [binary, str(micros), zone, str(ansi).lower(), expression],
        env={"PATH":"/usr/bin:/bin","TZ":"UTC"}, capture_output=True, timeout=5, text=True,
    )
    lines = result.stdout.splitlines()
    panic = next((name for name in (
        "Local time out of range for `NaiveDateTime`",
        "`NaiveDateTime - FixedOffset` out of range",
    ) if name in result.stderr or any(name in line for line in lines)), None)
    return {"id":name, "ansi":ansi, "input_micros":micros, "session":zone, "sql":expression,
            "exit_code":result.returncode,
            "phase":next((s for s in reversed(lines) if s in {"planning","physical","collect"}), None),
            "panic":panic,
            "outcome":next((s for s in lines if s.startswith(("error ","value "))), None)}


if __name__ == "__main__":
    binary, output = (Path(item).resolve() for item in sys.argv[1:])
    with output.open("x") as stream:
        for case in CASES:
            for ansi in (False, True):
                name, micros, zone, sql = case
                row = observe(str(binary), name, micros, zone, ansi, sql)
                stream.write(json.dumps(row, separators=(",", ":")) + "\n")
                if not ansi:
                    print(name, row["phase"], row["exit_code"], row["panic"] or row["outcome"])
