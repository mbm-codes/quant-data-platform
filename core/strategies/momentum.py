# core/strategies/momentum.py

from decimal import Decimal
from typing import Literal


def select_momentum_action(momentum_return: Decimal) -> Literal["BUY", "SELL", "HOLD"]:
    if momentum_return <= Decimal("-0.02"):
        return "SELL"
    if momentum_return >= Decimal("0.02"):
        return "BUY"

    return "HOLD"


def calculate_twenty_day_momentum(closes: list[Decimal]) -> Decimal:
    if len(closes) < 21:
        raise ValueError("At least 21 closing prices are required for 20-day momentum.")

    return Decimal((closes[-1] / closes[-21]) - 1)


def calculate_momentum_score(momentum_return: Decimal) -> Decimal:
    score = Decimal("0.5") + (momentum_return / Decimal("0.20"))

    return max(Decimal(0), min(score, Decimal(1)))
