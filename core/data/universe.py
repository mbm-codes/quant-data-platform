import csv
from pathlib import Path


def load_symbol_allowlist_csv(path: Path) -> set[str]:
    """Load a non-empty, unique NSE symbol allowlist from a CSV file."""
    with path.open(encoding="utf-8", newline="") as source_file:
        reader = csv.DictReader(source_file)

        if reader.fieldnames is None or "symbol" not in reader.fieldnames:
            raise ValueError("Symbol allowlist CSV must contain a 'symbol' column.")

        symbols: set[str] = set()

        for line_number, row in enumerate(reader, start=2):
            value = row["symbol"]

            if value is None or not value.strip():
                raise ValueError(
                    f"Symbol allowlist contains a blank symbol at line {line_number}."
                )

            symbol = value.strip().upper()

            if symbol in symbols:
                raise ValueError(
                    f"Symbol allowlist contains a duplicate symbol: {symbol}."
                )

            symbols.add(symbol)

    if not symbols:
        raise ValueError("Symbol allowlist must contain at least one symbol.")

    return symbols
