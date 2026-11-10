from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest

from core.data.duckdb import connect, register_daily_prices_view
from core.data.parquet import write_daily_prices_parquet
from core.data.price_history import (
    load_future_closes,
    load_historical_closes_by_symbol,
    load_recent_closes,
    load_recent_closes_by_symbol,
    load_symbol_closes,
)
from core.models.daily_prices import DailyPrice


def make_price(
    trade_date: date,
    close_price: Decimal,
    *,
    symbol: str = "RELIANCE",
    dataset_version: str = "test-v1",
) -> DailyPrice:
    return DailyPrice(
        exchange="NSE",
        symbol=symbol,
        trade_date=trade_date,
        open_price=close_price,
        high_price=close_price + Decimal(1),
        low_price=close_price - Decimal(1),
        close_price=close_price,
        volume_qty=1_000,
        source_name="test-source",
        source_file=f"{trade_date.isoformat()}.csv",
        source_hash="test-source-hash",
        ingested_at=datetime(2024, 1, 1, tzinfo=UTC),
        run_id="test-run",
        dataset_version="test-v1",
    )


def write_price_history(output_directory: Path) -> list[DailyPrice]:
    start_date = date(2024, 1, 1)

    prices = [
        make_price(
            trade_date=start_date + timedelta(days=index),
            close_price=Decimal(100) + Decimal(index),
        )
        for index in range(22)
    ]

    write_daily_prices_parquet(prices, output_directory)

    return prices


def test_load_recent_closes_returns_latest_twenty_one_in_order(
    tmp_path: Path,
) -> None:
    daily_prices_directory = tmp_path / "daily_prices"
    write_price_history(daily_prices_directory)

    connection = connect(tmp_path / "qdp.duckdb")

    try:
        register_daily_prices_view(connection, daily_prices_directory)

        closes = load_recent_closes(
            connection,
            exchange="NSE",
            symbol="reliance",
            dataset_version="test-v1",
            as_of=None,
        )
    finally:
        connection.close()

    assert len(closes) == 21
    assert closes[0] == (date(2024, 1, 2), Decimal("101.00000000"))
    assert closes[-1] == (date(2024, 1, 22), Decimal("121.00000000"))


def test_load_recent_closes_respects_as_of_date(tmp_path: Path) -> None:
    daily_prices_directory = tmp_path / "daily_prices"
    write_price_history(daily_prices_directory)

    connection = connect(tmp_path / "qdp.duckddb")
    try:
        register_daily_prices_view(connection, daily_prices_directory)

        closes = load_recent_closes(
            connection,
            exchange="NSE",
            symbol="RELIANCE",
            dataset_version="test-v1",
            as_of=date(2024, 1, 15),
        )
    finally:
        connection.close()

    assert len(closes) == 15
    assert closes[0] == (date(2024, 1, 1), Decimal("100.00000000"))
    assert closes[-1] == (date(2024, 1, 15), Decimal("114.00000000"))


def test_load_recent_closes_filters_by_dataset_version(tmp_path: Path) -> None:
    daily_prices_directory = tmp_path / "daily_prices"
    trade_date = date(2024, 1, 2)

    price_v1 = DailyPrice.model_validate(
        {
            **make_price(trade_date, Decimal(100)).model_dump(),
            "dataset_version": "test-v1",
            "source_file": "prices-v1.csv",
            "source_hash": "v1-hash",
        }
    )
    price_v2 = DailyPrice.model_validate(
        {
            **make_price(trade_date, Decimal(200)).model_dump(),
            "dataset_version": "test-v2",
            "source_file": "prices-v2.csv",
            "source_hash": "v2-hash",
        }
    )

    # Write separately: both versions intentionally share a business key.
    write_daily_prices_parquet([price_v1], daily_prices_directory)
    write_daily_prices_parquet([price_v2], daily_prices_directory)

    connection = connect(tmp_path / "qdp.duckdb")
    try:
        register_daily_prices_view(connection, daily_prices_directory)

        closes_v1 = load_recent_closes(
            connection,
            exchange="NSE",
            symbol="RELIANCE",
            dataset_version="test-v1",
            as_of=None,
        )
        closes_v2 = load_recent_closes(
            connection,
            exchange="NSE",
            symbol="RELIANCE",
            dataset_version="test-v2",
            as_of=None,
        )

        unknown_version = load_recent_closes(
            connection,
            exchange="NSE",
            symbol="RELIANCE",
            dataset_version="unknown",
            as_of=None,
        )
    finally:
        connection.close()

    assert closes_v1 == [(trade_date, Decimal(100))]
    assert closes_v2 == [(trade_date, Decimal(200))]
    assert unknown_version == []


def test_load_future_closes_returns_first_twenty_prices(
    tmp_path: Path,
) -> None:
    prices_directory = tmp_path / "daily_prices"
    reference_date = date(2024, 1, 1)

    prices = [
        make_price(
            trade_date=reference_date + timedelta(days=index),
            close_price=Decimal(100) + Decimal(index),
        )
        for index in range(23)
    ]
    write_daily_prices_parquet(prices, prices_directory)

    connection = connect(tmp_path / "qdp.duckdb")

    try:
        register_daily_prices_view(connection, prices_directory)

        closes = load_future_closes(
            connection,
            exchange="NSE",
            symbol="RELIANCE",
            dataset_version="test-v1",
            price_date=reference_date,
            evaluation_as_of_date=date(2024, 1, 23),
        )
    finally:
        connection.close()

    assert len(closes) == 20
    assert closes[0] == (date(2024, 1, 2), Decimal(101))
    assert closes[-1] == (date(2024, 1, 21), Decimal(120))


def test_load_future_closes_respects_evaluation_cutoff(
    tmp_path: Path,
) -> None:
    prices_directory = tmp_path / "daily_prices"
    reference_date = date(2024, 1, 1)

    prices = [
        make_price(
            trade_date=reference_date + timedelta(days=index),
            close_price=Decimal(100) + Decimal(index),
        )
        for index in range(23)
    ]
    write_daily_prices_parquet(prices, prices_directory)

    connection = connect(tmp_path / "qdp.duckdb")
    try:
        register_daily_prices_view(connection, prices_directory)

        closes = load_future_closes(
            connection,
            exchange="NSE",
            symbol="RELIANCE",
            dataset_version="test-v1",
            price_date=reference_date,
            evaluation_as_of_date=date(2024, 1, 11),
        )
    finally:
        connection.close()

    assert len(closes) == 10
    assert closes[0] == (date(2024, 1, 2), Decimal(101))
    assert closes[-1] == (date(2024, 1, 11), Decimal(110))


def test_load_future_closes_filters_dataset_version(
    tmp_path: Path,
) -> None:
    prices_directory = tmp_path / "daily_prices"
    reference_date = date(2024, 1, 1)

    selected_price = make_price(
        trade_date=date(2024, 1, 2),
        close_price=Decimal(101),
    )
    other_price = make_price(
        trade_date=date(2024, 1, 3),
        close_price=Decimal(102),
    ).model_copy(update={"dataset_version": "other-v1"})

    write_daily_prices_parquet(
        [selected_price, other_price],
        prices_directory,
    )

    connection = connect(tmp_path / "qdp.duckdb")
    try:
        register_daily_prices_view(connection, prices_directory)

        closes = load_future_closes(
            connection,
            exchange="NSE",
            symbol="RELIANCE",
            dataset_version="test-v1",
            price_date=reference_date,
            evaluation_as_of_date=date(2024, 1, 3),
        )
    finally:
        connection.close()

    assert closes == [(date(2024, 1, 2), Decimal(101))]


def test_load_future_closes_rejects_duplicate_on_twentieth_date() -> None:
    connection = duckdb.connect(":memory:")
    try:
        connection.execute(
            """
            CREATE TABLE daily_prices (
                exchange VARCHAR,
                symbol VARCHAR,
                dataset_version VARCHAR,
                trade_date DATE,
                close_price DECIMAL(20,8)
            )
            """
        )

        reference_date = date(2024, 1, 1)
        rows = [
            (
                "NSE",
                "RELIANCE",
                "test-v1",
                reference_date + timedelta(days=index),
                Decimal(100) + Decimal(index),
            )
            for index in range(1, 21)
        ]
        rows.append(("NSE", "RELIANCE", "test-v1", date(2024, 1, 21), Decimal(999)))

        connection.executemany(
            "INSERT INTO daily_prices VALUES(?, ?, ?, ?, ?)",
            rows,
        )

        with pytest.raises(ValueError, match="Duplicate daily prices"):
            load_future_closes(
                connection,
                exchange="NSE",
                symbol="RELIANCE",
                dataset_version="test-v1",
                price_date=reference_date,
                evaluation_as_of_date=date(2024, 1, 21),
            )
    finally:
        connection.close()


def test_load_symbol_closes_returns_ordered_prices_for_dataset(
    tmp_path: Path,
) -> None:
    prices_directory = tmp_path / "daily_prices"

    prices = [
        make_price(trade_date=date(2024, 1, 2), close_price=Decimal(101)),
        make_price(trade_date=date(2024, 1, 1), close_price=Decimal(100)),
        make_price(trade_date=date(2024, 1, 3), close_price=Decimal(102)).model_copy(
            update={"dataset_version": "other-v1"}
        ),
    ]
    write_daily_prices_parquet(prices, prices_directory)

    connection = connect(tmp_path / "qdp.duckdb")
    try:
        register_daily_prices_view(connection, prices_directory)

        closes = load_symbol_closes(
            connection,
            exchange="nse",
            symbol="reliance",
            dataset_version="test-v1",
        )
    finally:
        connection.close()

    assert closes == [
        (date(2024, 1, 1), Decimal(100)),
        (date(2024, 1, 2), Decimal(101)),
    ]


def test_load_recent_closes_by_symbol_returns_each_symbol_window(
    tmp_path: Path,
) -> None:
    prices_directory = tmp_path / "daily_prices"
    start_date = date(2024, 1, 1)

    prices: list[DailyPrice] = []

    for index in range(21):
        trade_date = start_date + timedelta(days=index)

        reliance_price = make_price(
            trade_date=trade_date,
            close_price=Decimal(100) + Decimal(index),
        )

        prices.append(reliance_price)

        prices.append(
            reliance_price.model_copy(
                update={
                    "symbol": "TCS",
                    "source_file": "tcs.csv",
                }
            )
        )

    for index in range(20):
        incomplete_price = make_price(
            trade_date=start_date + timedelta(days=index),
            close_price=Decimal(200) + Decimal(index),
        ).model_copy(
            update={
                "symbol": "INCOMPLETE",
                "source_file": "incomplete.csv",
            }
        )
        prices.append(incomplete_price)

    prices.append(
        make_price(
            trade_date=date(2024, 1, 22),
            close_price=Decimal(999),
        )
    )

    write_daily_prices_parquet(prices, prices_directory)

    connection = connect(tmp_path / "qdp.duckdb")
    try:
        register_daily_prices_view(connection, prices_directory)

        closes_by_symbol = load_recent_closes_by_symbol(
            connection,
            exchange="NSE",
            dataset_version="test-v1",
            as_of_date=date(2024, 1, 21),
        )
    finally:
        connection.close()

    assert set(closes_by_symbol) == {
        "INCOMPLETE",
        "RELIANCE",
        "TCS",
    }
    assert len(closes_by_symbol["RELIANCE"]) == 21
    assert len(closes_by_symbol["TCS"]) == 21
    assert len(closes_by_symbol["INCOMPLETE"]) == 20

    assert closes_by_symbol["RELIANCE"][0] == (
        date(2024, 1, 1),
        Decimal(100),
    )
    assert closes_by_symbol["RELIANCE"][-1] == (
        date(2024, 1, 21),
        Decimal(120),
    )


def test_loads_historical_closes_for_allowed_symbols(
    tmp_path: Path,
) -> None:
    daily_prices_directory = tmp_path / "daily_prices"

    write_daily_prices_parquet(
        [
            make_price(
                symbol="ALPHA",
                trade_date=date(2024, 1, 1),
                close_price=Decimal(100),
            ),
            make_price(
                symbol="ALPHA",
                trade_date=date(2024, 1, 2),
                close_price=Decimal(101),
            ),
            make_price(
                symbol="ALPHA",
                trade_date=date(2024, 1, 3),
                close_price=Decimal(102),
            ),
            make_price(
                symbol="BETA",
                trade_date=date(2024, 1, 1),
                close_price=Decimal(200),
            ),
            make_price(
                symbol="BETA",
                trade_date=date(2024, 1, 2),
                close_price=Decimal(201),
            ),
            make_price(
                symbol="GAMMA",
                trade_date=date(2024, 1, 1),
                close_price=Decimal(300),
            ),
        ],
        daily_prices_directory,
    )

    connection = connect(tmp_path / "qdp.duckdb")
    try:
        register_daily_prices_view(connection, daily_prices_directory)

        closes_by_symbol = load_historical_closes_by_symbol(
            connection,
            exchange="NSE",
            dataset_version="test-v1",
            evaluation_as_of_date=date(2024, 1, 2),
            allowed_symbols={"ALPHA", "BETA"},
        )
    finally:
        connection.close()

    assert closes_by_symbol == {
        "ALPHA": [
            (date(2024, 1, 1), Decimal("100.00000000")),
            (date(2024, 1, 2), Decimal("101.00000000")),
        ],
        "BETA": [
            (date(2024, 1, 1), Decimal("200.00000000")),
            (date(2024, 1, 2), Decimal("201.00000000")),
        ],
    }
