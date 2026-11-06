from pathlib import Path

from core.data.universe import load_symbol_allowlist_csv


def resolve_symbol_universe(
    symbols_file: Path | None,
) -> set[str] | None:
    """Resolve an optional explicit symbol universe from a CSV file."""
    if symbols_file is None:
        return None

    return load_symbol_allowlist_csv(symbols_file)
