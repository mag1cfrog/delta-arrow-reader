"""Balance preparation positions against both four-process execution orders."""

from collections import Counter
from itertools import product


ORDERS = ((0, 2, 3, 1), (2, 0, 1, 3))  # ABBA and BAAB; slots 0/1 are A.
CONTROLS = ("same-before", "same-after")


def make_schedule(include_comparison=False):
    schedule = []
    for block in range(4):
        scenarios = list(reversed(CONTROLS) if block % 2 else CONTROLS)
        if include_comparison:
            scenarios.insert(1, "paired")
        for scenario in scenarios:
            for index in range(4 if scenario == "paired" else 2):
                rotation = (2 * block + index) % 4
                direction = (block // 2 + index) % 2
                schedule.append({
                    "block": block,
                    "scenario": scenario,
                    "preparation_order": [(rotation + slot) % 4 for slot in range(4)],
                    "execution_order": list(ORDERS[direction]),
                })
    check_schedule(schedule)
    return schedule


def check_schedule(schedule):
    scenarios = {row["scenario"] for row in schedule}
    assert scenarios in [set(CONTROLS), set(CONTROLS) | {"paired"}]
    for row in schedule:
        assert row["block"] in range(4)
        assert sorted(row["preparation_order"]) == list(range(4))
        assert tuple(row["execution_order"]) in ORDERS
    for scenario in scenarios:
        rows = [row for row in schedule if row["scenario"] == scenario]
        per_block = 4 if scenario == "paired" else 2
        assert len(rows) == 4 * per_block
        for block in range(4):
            assert Counter(tuple(row["execution_order"]) for row in rows if row["block"] == block) == {
                order: per_block // 2 for order in ORDERS
            }
        expected = {combination: len(rows) // 8 for combination in product(range(4), ORDERS)}
        for slot in range(4):
            observed = Counter((row["preparation_order"].index(slot), tuple(row["execution_order"])) for row in rows)
            assert observed == expected, ("not fully crossed", scenario, slot, observed)


if __name__ == "__main__":
    import json
    from pathlib import Path

    controls = make_schedule()
    comparison = make_schedule(include_comparison=True)
    assert len(controls) == 16 and len(comparison) == 32

    # The recorded schedule had balanced margins but omitted joint combinations.
    historical = json.loads(Path(__file__).with_name("float-type-prepared-phases-results.json").read_text())
    try:
        check_schedule(historical["method"]["schedule"])
    except AssertionError as error:
        assert "not fully crossed" in str(error)
    else:
        raise AssertionError("The historical counterexample must be rejected")
    try:
        check_schedule(controls[:-1])
    except AssertionError:
        pass
    else:
        raise AssertionError("An incomplete schedule must be rejected")
    print("Both schedules are fully crossed; historical and incomplete schedules rejected")
