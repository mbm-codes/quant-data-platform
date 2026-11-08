from dataclasses import dataclass

import duckdb

from core.models.ranking_snapshot import (
    MomentumRankingRun,
    MomentumRankingSnapshotCandidate,
)

RANKING_RUN_COLUMNS = (
    "ranking_run_id",
    "exchange",
    "as_of_date",
    "generated_at",
    "strategy_name",
    "strategy_version",
    "input_dataset_version",
    "universe_hash",
    "max_price_age_days",
    "eligible_candidate_count",
)

RANKING_CANDIDATE_COLUMNS = (
    "ranking_run_id",
    "rank",
    "exchange",
    "symbol",
    "as_of_date",
    "price_date",
    "reference_price",
    "momentum_return",
    "score",
    "action",
    "strategy_name",
    "strategy_version",
    "input_dataset_version",
)


@dataclass(frozen=True)
class LoadedMomentumRankingSnapshot:
    """One recorded ranking run with its ordered candidate universe."""

    ranking_run: MomentumRankingRun
    candidates: list[MomentumRankingSnapshotCandidate]


def load_momentum_ranking_snapshot(
    connection: duckdb.DuckDBPyConnection,
    *,
    ranking_run_id: str,
) -> LoadedMomentumRankingSnapshot:
    """Load exactly one ranking run and its full ordered candidate universe."""
    run_columns_sql = ", ".join(RANKING_RUN_COLUMNS)
    run_rows = connection.execute(
        f"""
        SELECT {run_columns_sql}
        FROM momentum_ranking_runs
        WHERE ranking_run_id = ?
        LIMIT 2
        """,
        [ranking_run_id],
    ).fetchall()

    if not run_rows:
        raise ValueError(f"Ranking run not found: {ranking_run_id}")

    if len(run_rows) > 1:
        raise ValueError(f"Duplicate ranking runs found: {ranking_run_id}")

    ranking_run = MomentumRankingRun.model_validate(
        dict(zip(RANKING_RUN_COLUMNS, run_rows[0], strict=True))
    )

    candidate_columns_sql = ", ".join(RANKING_CANDIDATE_COLUMNS)
    candidate_rows = connection.execute(
        f"""
        SELECT {candidate_columns_sql}
        FROM momentum_ranking_candidates
        WHERE ranking_run_id = ?
        ORDER BY rank ASC
        """,
        [ranking_run_id],
    ).fetchall()

    candidates = [
        MomentumRankingSnapshotCandidate.model_validate(
            dict(zip(RANKING_CANDIDATE_COLUMNS, row, strict=True))
        )
        for row in candidate_rows
    ]

    if len(candidates) != ranking_run.eligible_candidate_count:
        raise ValueError(
            "Ranking snapshot candidate count does not match run metadata."
        )

    return LoadedMomentumRankingSnapshot(
        ranking_run=ranking_run,
        candidates=candidates,
    )
