# tests/v0/test_recommendation_evaluation.py

from decimal import Decimal

import pytest

from core.recommendation_evaluation import calculate_forward_return


@pytest.mark.parametrize(
    ("reference_price", "future_price", "expected_return"),
    [
        (Decimal(100), Decimal(110), Decimal("0.10")),
        (Decimal(100), Decimal(90), Decimal("-0.10")),
        (Decimal(100), Decimal(100), Decimal(0)),
    ],
)
def test_calculated_forward_return(
    reference_price: Decimal,
    future_price: Decimal,
    expected_return: Decimal,
) -> None:
    result = calculate_forward_return(reference_price, future_price)

    assert result == expected_return


@pytest.mark.parametrize(
    ("reference_price", "future_price"),
    [
        (Decimal(0), Decimal(110)),
        (Decimal(100), Decimal(0)),
    ],
)
def test_rejects_non_positive_prices(
    reference_price: Decimal,
    future_price: Decimal,
) -> None:
    with pytest.raises(ValueError):
        calculate_forward_return(reference_price, future_price)
