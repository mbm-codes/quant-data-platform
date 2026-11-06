from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from core.data.parquet import write_daily_prices_parquet
from core.models.daily_prices import DailyPrice
from core.services.ranking import rank_momentum_universe


def make_price(
    symbol: str,
    trade_date: date,
    close_price: Decimal,
) -> DailyPrice:
    return DailyPrice(
        exchange="NSE",
        symbol=symbol,
        trade_date=trade_date,
        open_price=close_price,
        high_price=close_price,
        low_price=close_price,
        close_price=close_price,
        volume_qty=1_000,
        source_name="test-source",
        source_file=f"{symbol}.csv",
        source_hash=f"{symbol}-source-hash",
        ingested_at=datetime(2024, 1, 1, tzinfo=UTC),
        run_id="test-run",
        dataset_version="test-v1",
    )


def symbol_history(
    symbol: str,
    start_date: date,
    first_close: str,
    final_close: str,
) -> list[DailyPrice]:
    return [
        make_price(
            symbol,
            start_date + timedelta(days=index),
            Decimal(first_close if index == 0 else final_close),
        )
        for index in range(21)
    ]


def test_ranks_fresh_eligible_symbols_and_excludes_stale_symbols(
    tmp_path: Path,
) -> None:
    prices_directory = tmp_path / "daily_prices"

    prices = [
        *symbol_history(
            "BUY_SYMBOL",
            date(2024, 1, 1),
            "100",
            "120",
        ),
        *symbol_history(
            "SELL_SYMBOL",
            date(2024, 1, 1),
            "100",
            "90",
        ),
        *symbol_history(
            "STALE_SYMBOL",
            date(2023, 12, 1),
            "100",
            "130",
        ),
    ]
    write_daily_prices_parquet(prices, prices_directory)

    candidates = rank_momentum_universe(
        exchange="NSE",
        as_of_date=date(2024, 1, 21),
        input_dataset_version="test-v1",
        database_path=tmp_path / "qdp.duckdb",
        daily_prices_directory=prices_directory,
        max_price_age_days=7,
    )

    assert [candidate.symbol for candidate in candidates] == [
        "BUY_SYMBOL",
        "SELL_SYMBOL",
    ]
    assert [candidate.rank for candidate in candidates] == [1, 2]
    assert [candidate.action for candidate in candidates] == ["BUY", "SELL"]


def test_rejects_negative_max_price_age_days(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="max_price_age_days cannot be negative."):
        rank_momentum_universe(
            exchange="NSE",
            as_of_date=date(2024, 1, 21),
            input_dataset_version="test-v1",
            database_path=tmp_path / "qdp.duckdb",
            daily_prices_directory=tmp_path / "daily_prices",
            max_price_age_days=-1,
        )


def test_filters_ranked_candidates_to_allowlist(tmp_path: Path) -> None:
    prices_directory = tmp_path / "daily_prices"
    prices = [
        *symbol_history(
            "BUY_SYMBOL",
            date(2024, 1, 1),
            "100",
            "120",
        ),
        *symbol_history(
            "SELL_SYMBOL",
            date(2024, 1, 1),
            "100",
            "90",
        ),
    ]
    write_daily_prices_parquet(prices, prices_directory)

    candidates = rank_momentum_universe(
        exchange="NSE",
        as_of_date=date(2024, 1, 21),
        input_dataset_version="test-v1",
        database_path=tmp_path / "qdp.duckdb",
        daily_prices_directory=prices_directory,
        allowed_symbols={"sell_symbol"},
    )

    assert len(candidates) == 1
    assert candidates[0].rank == 1
    assert candidates[0].symbol == "SELL_SYMBOL"
    assert candidates[0].action == "SELL"
