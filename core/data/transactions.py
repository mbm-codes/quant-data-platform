import csv
from datetime import datetime
from pathlib import Path

from core.models.transactions import PortfolioTransaction

REQUIRED_COLUMNS = {
    "transaction_id",
    "account_id",
    "symbol",
    "trade_date",
    "side",
    "quantity",
    "unit_price",
    "fees",
}


def load_portfolio_transactions_csv(
    path: Path,
    *,
    run_id: str,
    ingested_at: datetime,
) -> list[PortfolioTransaction]:
    """Load and validate a user-maintained portfolio transaction CSV."""
    with path.open(encoding="utf-8", newline="") as source_file:
        reader = csv.DictReader(source_file)
        fieldnames = set(reader.fieldnames or [])
        missing_columns = REQUIRED_COLUMNS - fieldnames
        unexpected_columns = fieldnames - REQUIRED_COLUMNS

        if missing_columns or unexpected_columns:
            raise ValueError(
                f"Unexpected columns in {path.name}: {reader.fieldnames}"
            )

        transactions: list[PortfolioTransaction] = []
        transaction_ids: set[str] = set()

        for line_number, row in enumerate(reader, start=2):
            try:
                transaction = PortfolioTransaction.model_validate(
                    {
                        **row,
                        "source_file": path.name,
                        "ingested_at": ingested_at,
                        "run_id": run_id,
                    }
                )
            except ValueError as error:
                raise ValueError(
                    f"Invalid portfolio transaction at line {line_number}: {error}"
                ) from error

            if transaction.transaction_id in transaction_ids:
                raise ValueError(
                    f"Duplicate transaction ID at line {line_number}: "
                    f"{transaction.transaction_id}"
                )

            transaction_ids.add(transaction.transaction_id)
            transactions.append(transaction)

    return transactions