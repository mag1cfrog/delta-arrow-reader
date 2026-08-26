"""Counterbalanced order used for each three-reader workload."""

CANDIDATES = ("delta-arrow-reader", "delta-rs", "duckdb")
RUN_ORDER = (
    ("delta-arrow-reader", "delta-rs", "duckdb"),
    ("delta-rs", "duckdb", "delta-arrow-reader"),
    ("duckdb", "delta-arrow-reader", "delta-rs"),
    ("duckdb", "delta-rs", "delta-arrow-reader"),
    ("delta-rs", "delta-arrow-reader", "duckdb"),
    ("delta-arrow-reader", "duckdb", "delta-rs"),
)


def validate() -> None:
    assert all(set(run) == set(CANDIDATES) for run in RUN_ORDER)
    for position in range(len(CANDIDATES)):
        assert all(
            sum(run[position] == candidate for run in RUN_ORDER) == 2
            for candidate in CANDIDATES
        )


if __name__ == "__main__":
    validate()
    for run in RUN_ORDER:
        print(" -> ".join(run))
