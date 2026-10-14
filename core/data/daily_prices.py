import csv
from pathlib import Path

from core.models.daily_prices import DailyPrice


def load_daily_prices_csv(path: Path) -> list[DailyPrice]:
    """Load and validate canonical daily-price records from a CSV
    file."""

    with path.open(encoding="utf-8", newline="") as source_file:
        return [
            DailyPrice.model_validate(row)
            for row in csv.DictReader(source_file)
        ]



    