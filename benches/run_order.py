"""Counterbalanced orders used for the reader comparison."""

from collections import Counter


WORKLOAD_CANDIDATES = {
    "mixed-column": ("delta-arrow-reader", "delta-rs", "duckdb", "polars"),
    "text": (
        "delta-arrow-reader",
        "delta-rs",
        "duckdb",
        "polars",
        "daft",
    ),
    "dv-limit": ("delta-arrow-reader", "delta-rs", "duckdb", "polars"),
    "dv-full": ("delta-arrow-reader", "delta-rs", "duckdb", "polars"),
}


def balanced_orders(candidates: tuple[str, ...]) -> list[tuple[str, ...]]:
    """Represent every position and ordered adjacency twice."""
    count = len(candidates)
    first = [0]
    for position in range(1, count):
        first.append((position + 1) // 2 if position % 2 else count - position // 2)
    rows = [
        tuple(candidates[(index + shift) % count] for index in first)
        for shift in range(count)
    ]
    return rows + [tuple(reversed(row)) for row in rows]


def validate() -> None:
    for candidates in WORKLOAD_CANDIDATES.values():
        orders = balanced_orders(candidates)
        positions = Counter(
            (candidate, position)
            for order in orders
            for position, candidate in enumerate(order)
        )
        adjacent_pairs = Counter(
            pair for order in orders for pair in zip(order, order[1:])
        )
        assert set(positions.values()) == {2}
        assert set(adjacent_pairs.values()) == {2}
        assert len(adjacent_pairs) == len(candidates) * (len(candidates) - 1)


if __name__ == "__main__":
    validate()
    for workload, candidates in WORKLOAD_CANDIDATES.items():
        print(f"[{workload}]")
        for order in balanced_orders(candidates):
            print(" -> ".join(order))
