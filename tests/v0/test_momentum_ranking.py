from datetime import date, timedelta
from decimal import Decimal

from core.strategies.momentum import rank_momentum_candidates


def make_closes(
    first_close: str,
    final_close: str,
) -> list[tuple[date, Decimal]]:
    start_date = date(2024, 1, 1)

    return [
        (start_date, Decimal(first_close)),
        *[
            (
                start_date + timedelta(days=index),
                Decimal(final_close),
            )
            for index in range(1, 21)
        ],
    ]


def test_ranks_eligible_symbols_by_momentum_score() -> None:
    candidates = rank_momentum_candidates(
        exchange="NSE",
        as_of_date=date(2024, 1, 21),
        input_dataset_version="test-v1",
        closes_by_symbol={
            "SELL_SYMBOL": make_closes("100", "90"),
            "BUY_SYMBOL": make_closes("100", "120"),
            "HOLD_SYMBOL": make_closes("100", "101"),
            "INCOMPLETE": make_closes("100", "110")[:20],
        },
    )

    assert [candidate.symbol for candidate in candidates] == [
        "BUY_SYMBOL",
        "HOLD_SYMBOL",
        "SELL_SYMBOL",
    ]
    assert [candidate.rank for candidate in candidates] == [1, 2, 3]
    assert [candidate.action for candidate in candidates] == [
        "BUY",
        "HOLD",
        "SELL",
    ]
    assert candidates[0].momentum_return == Decimal("0.20")
    assert candidates[0].score == Decimal(1)
