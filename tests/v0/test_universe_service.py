from pathlib import Path

from core.services.universe import resolve_symbol_universe


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
