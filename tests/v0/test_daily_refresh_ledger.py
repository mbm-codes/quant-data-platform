from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from core.data.daily_refresh_ledger import write_daily_refresh_ledger
from core.models.daily_refresh import DailyRefreshResult


def make_daily_refresh_result() -> DailyRefreshResult:
    return DailyRefreshResult(
        run_id="daily-refresh-001",
        status="COMPLETED",
        started_at=datetime(2026, 1, 20, 9, tzinfo=UTC),
        finished_at=datetime(2026, 1, 20, 9, 2, tzinfo=UTC),
        files_discovered=2,
        files_processed=2,
        files_failed=0,
        duplicate_files_skipped=0,
        valid_rows=3_000,
        rejected_rows=0,
        freshness_status="FRESH",
        quality_status="HEALTHY",
    )


def test_writes_immutable_daily_refresh_result(
    tmp_path: Path,
) -> None:
    result = make_daily_refresh_result()

    written_paths = write_daily_refresh_ledger([result], tmp_path)

    assert len(written_paths) == 1
    assert written_paths[0].parent == tmp_path / "refresh_year=2026"
    assert written_paths[0].suffix == ".parquet"

    table = pq.read_table(written_paths[0])

    assert table.num_rows == 1
    assert table.column("run_id").to_pylist() == ["daily-refresh-001"]
    assert table.column("status").to_pylist() == ["COMPLETED"]
    assert table.column("freshness_status").to_pylist() == ["FRESH"]
    assert table.column("quality_status").to_pylist() == ["HEALTHY"]


def test_rejects_duplicate_daily_refresh_result(
    tmp_path: Path,
) -> None:
    result = make_daily_refresh_result()

    write_daily_refresh_ledger([result], tmp_path)

    with pytest.raises(ValueError, match="already exists"):
        write_daily_refresh_ledger([result], tmp_path)
