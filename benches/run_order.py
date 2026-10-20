"""Counterbalanced orders used for the reader comparison."""

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
