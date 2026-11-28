"""Add the existing mixed DATE/DECIMAL control to the frozen SF1 case selection."""

import sys

sys.dont_write_bytecode = True

import sf1


select_baseline = sf1.manifest


def select(protocol):
    frozen = sf1.BENCHES / "casts/date/queries.json"
    assert sf1.digest(frozen) == protocol["date_cast_manifest_sha256"]
    control = protocol["mixed_date_control"]
    assert control["suite"] == "casts-date" and control["id"] == "cast_strings"
    specs = select_baseline(protocol)
    reference, = [spec for spec in specs if spec["suite"] == "casts-date"
                  and spec["id"] == "cast_date" and spec["configuration"] == "native"]
    query, = [row for row in sf1.cells.read(frozen)["queries"] if row["id"] == control["id"]]
    assert all(query[key].endswith(" FROM bench") and "CAST(date_text AS DATE)" in query[key]
               for key in ("native_sql", "spark_sql"))
    for mode in control["configurations"]:
        assert mode in sf1.cells.CONFIGURATIONS["casts-date"]
        specs.append({**reference, "id": "cast_strings", "configuration": mode,
                      "label": f"casts-date/cast_strings/{mode}"})
    assert len({spec["label"] for spec in specs}) == len(specs)
    return specs


if __name__ == "__main__":
    sf1.manifest = select
    sf1.main()
