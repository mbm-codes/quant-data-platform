import hashlib
from collections.abc import Iterable
from pathlib import Path

from core.data.universe import load_symbol_allowlist_csv


def resolve_symbol_universe(
    symbols_file: Path | None,
) -> set[str] | None:
    """Resolve an optional explicit symbol universe from a CSV file."""
    if symbols_file is None:
        return None

    return load_symbol_allowlist_csv(symbols_file)


def calculate_universe_hash(symbols: Iterable[str]) -> str:
    """Return a stable SHA-256 hash for a normalized symbol universe."""
    normalized_symbols = sorted({symbol.strip().upper() for symbol in symbols})
    serialized_symbols = "\n".join(normalized_symbols)

    return hashlib.sha256(serialized_symbols.encode("utf-8")).hexdigest()
