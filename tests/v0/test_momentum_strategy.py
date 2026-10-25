from decimal import Decimal

import pytest

from core.strategies.momentum import (
    calculate_momentum_score,
    calculate_twenty_day_momentum,
    select_momentum_action,
)


@pytest.mark.parametrize(
    ("momentum_return", "expected_action"),
    [
        (Decimal("0.02"), "BUY"),
        (Decimal("-0.02"), "SELL"),
        (Decimal(0), "HOLD"),
        (Decimal("0.0199"), "HOLD"),
        (Decimal("-0.0199"), "HOLD"),
    ],
)
def test_select_momentum_action(
    momentum_return: Decimal,
    expected_action: str,
) -> None:
    assert select_momentum_action(momentum_return) == expected_action


def test_calculate_twenty_day_momentum() -> None:
    closes = [Decimal("100.00")] + [Decimal("101.00")] * 19 + [Decimal("121.00")]

    assert calculate_twenty_day_momentum(closes) == Decimal("0.21")


def test_uses_most_recent_twenty_one_closes() -> None:
    closes = (
        [Decimal("50.00")]
        + [Decimal("100.00")]
        + [Decimal("101.00")] * 19
        + [Decimal("121.00")]
    )
    assert calculate_twenty_day_momentum(closes) == Decimal("0.21")


def test_rejects_fewer_than_twenty_one_closes() -> None:
    closes = [Decimal("100.00")] * 20

    with pytest.raises(ValueError, match="21"):
        calculate_twenty_day_momentum(closes)


@pytest.mark.parametrize(
    ("momentum_return", "expected_score"),
    [
        (Decimal("-0.10"), Decimal(0)),
        (Decimal("-0.02"), Decimal("0.4")),
        (Decimal(0), Decimal("0.5")),
        (Decimal("0.02"), Decimal("0.6")),
        (Decimal("0.10"), Decimal(1)),
        (Decimal("0.20"), Decimal(1)),
    ],
)
def test_calculate_momentum_score(
    momentum_return: Decimal,
    expected_score: Decimal,
) -> None:
    assert calculate_momentum_score(momentum_return) == expected_score
