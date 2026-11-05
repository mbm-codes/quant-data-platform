# core/recommendation_evaluation.py

from decimal import Decimal


def calculate_forward_return(
    reference_price: Decimal,
    future_price: Decimal,
) -> Decimal:
    if reference_price <= 0 or future_price <= 0:
        raise ValueError("Reference and future prices must be positive.")

    return (future_price / reference_price) - Decimal(1)
