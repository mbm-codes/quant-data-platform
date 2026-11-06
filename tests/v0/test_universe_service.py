from pathlib import Path

from core.services.universe import (
    calculate_universe_hash,
    resolve_symbol_universe,
)


def test_returns_none_when_no_universe_file_is_provided() -> None:
    assert resolve_symbol_universe(None) is None


def test_resolves_symbols_from_allowlist_file(tmp_path: Path) -> None:
    source = tmp_path / "universe"
    source.write_text(
        "symbol\nreliance\nTCS\n",
        encoding="utf-8",
    )

    symbols = resolve_symbol_universe(source)

    assert symbols == {"RELIANCE", "TCS"}


def test_calculates_order_independent_universe_hash() -> None:
    first_hash = calculate_universe_hash(["RELIANCE", "tcs", "INFY"])
    second_hash = calculate_universe_hash(["infy", "RELIANCE", "TCS", "TCS"])

    assert first_hash == second_hash
    assert len(first_hash) == 64


def test_calculates_different_hash_for_different_universe() -> None:
    first_hash = calculate_universe_hash(["RELIANCE", "TCS"])
    second_hash = calculate_universe_hash(["RELIANCE, INFY"])

    assert first_hash != second_hash
