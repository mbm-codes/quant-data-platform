from datetime import datetime
from pathlib import Path

from core.data.kaggle_inventory import (
    HistoricalSourceInventory,
    inventory_kaggle_historical_directory,
    write_historical_source_inventory,
)


def inventory_historical(
    source_directory: Path,
    output_path: Path,
    *,
    run_id: str,
    dataset_version: str,
    ingested_at: datetime,
) -> HistoricalSourceInventory:

    inventory = inventory_kaggle_historical_directory(
        source_directory,
        run_id=run_id,
        dataset_version=dataset_version,
        ingested_at=ingested_at,
    )

    write_historical_source_inventory(inventory, output_path)
    return inventory
