import hashlib
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from core.models.ranking_snapshot import (
    MomentumRankingRun,
    MomentumRankingSnapshotCandidate,
)

RANKING_RUN_SCHEMA = pa.schema(
    [
        pa.field("ranking_run_id", pa.string(), nullable=False),
        pa.field("exchange", pa.string(), nullable=False),
        pa.field("as_of_date", pa.date32(), nullable=False),
        pa.field("generated_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("strategy_name", pa.string(), nullable=False),
        pa.field("strategy_version", pa.string(), nullable=False),
        pa.field("input_dataset_version", pa.string(), nullable=False),
        pa.field("universe_hash", pa.string(), nullable=False),
        pa.field("max_price_age_days", pa.int64(), nullable=False),
        pa.field("eligible_candidate_count", pa.int64(), nullable=False),
    ]
)

RANKING_CANDIDATE_SCHEMA = pa.schema(
    [
        pa.field("ranking_run_id", pa.string(), nullable=False),
        pa.field("rank", pa.int64(), nullable=False),
        pa.field("exchange", pa.string(), nullable=False),
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("as_of_date", pa.date32(), nullable=False),
        pa.field("price_date", pa.date32(), nullable=False),
        pa.field("reference_price", pa.decimal128(20, 8), nullable=False),
        pa.field("momentum_return", pa.decimal128(20, 8), nullable=False),
        pa.field("score", pa.decimal128(10, 8), nullable=False),
        pa.field("action", pa.string(), nullable=False),
        pa.field("strategy_name", pa.string(), nullable=False),
        pa.field("strategy_version", pa.string(), nullable=False),
        pa.field("input_dataset_version", pa.string(), nullable=False),
    ]
)


def write_momentum_ranking_snapshot(
    ranking_run: MomentumRankingRun,
    candidates: list[MomentumRankingSnapshotCandidate],
    output_directory: Path,
) -> tuple[Path, Path]:
    """Write one immutable ranking run and its ranked candidates."""
    _validate_snapshot(ranking_run, candidates)

    run_directory = (
        output_directory / "runs" / f"as_of_year={ranking_run.as_of_date.year}"
    )
    candidate_directory = (
        output_directory / "candidates" / f"as_of_year={ranking_run.as_of_date.year}"
    )
    run_directory.mkdir(parents=True, exist_ok=True)
    candidate_directory.mkdir(parents=True, exist_ok=True)

    filename = _snapshot_filename(ranking_run.ranking_run_id)
    run_path = run_directory / filename
    candidate_path = candidate_directory / filename

    if run_path.exists() or candidate_path.exists():
        raise ValueError(
            f"Ranking snapshot already exists: {ranking_run.ranking_run_id}"
        )

    run_table = pa.Table.from_pylist(
        [ranking_run.model_dump(mode="python")],
        schema=RANKING_RUN_SCHEMA,
    )
    candidate_table = pa.Table.from_pylist(
        [candidate.model_dump(mode="python") for candidate in candidates],
        schema=RANKING_CANDIDATE_SCHEMA,
    )

    _write_table(run_table, run_path)
    _write_table(candidate_table, candidate_path)

    return run_path, candidate_path


def _validate_snapshot(
    ranking_run: MomentumRankingRun,
    candidates: list[MomentumRankingSnapshotCandidate],
) -> None:
    if not candidates:
        raise ValueError("Ranking snapshots require at least one candidate.")

    if ranking_run.eligible_candidate_count != len(candidates):
        raise ValueError("eligible_candidate_count must match the candidate count.")

    if any(
        candidate.ranking_run_id != ranking_run.ranking_run_id
        for candidate in candidates
    ):
        raise ValueError("All snapshot candidates must use the ranking run ID.")

    ranks = [candidate.rank for candidate in candidates]
    if ranks != list(range(1, len(candidates) + 1)):
        raise ValueError("Snapshot candidate ranks must be sequential from one.")

    symbols = [candidate.symbol for candidate in candidates]
    if len(symbols) != len(set(symbols)):
        raise ValueError("Snapshot candidates must have unique symbols.")


def _write_table(
    table: pa.Table,
    output_path: Path,
) -> None:
    temporary_path = output_path.with_suffix(".tmp")

    pq.write_table(table, temporary_path)
    temporary_path.replace(output_path)


def _snapshot_filename(ranking_run_id: str) -> str:
    identifier_hash = hashlib.sha256(ranking_run_id.encode("utf-8")).hexdigest()
    return f"ranking_run_id={identifier_hash}.parquet"
