from pathlib import Path

import pytest

from core.data.universe import load_symbol_allowlist_csv


def test_loads_and_normalizes_symbol_allowlist(tmp_path: Path) -> None:
    source = tmp_path / "universe.csv"
    source.write_text(
        "symbol\nreliance\nTCS\nINFY\n",
        encoding="utf-8",
    )

    symbols = load_symbol_allowlist_csv(source)

    assert symbols == {"RELIANCE", "TCS", "INFY"}


def test_rejects_duplicate_symbol(tmp_path: Path) -> None:
    source = tmp_path / "universe.csv"
    source.write_text(
        "symbol\nRELIANCE\nreliance\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="duplicate symbol: RELIANCE"):
        load_symbol_allowlist_csv(source)


def test_rejects_missing_symbol_column(tmp_path: Path) -> None:
    source = tmp_path / "universe.csv"
    source.write_text(
        "ticker\nRELIANCE\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="'symbol' column"):
        load_symbol_allowlist_csv(source)
