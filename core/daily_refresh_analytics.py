from datetime import datetime
from decimal import Decimal

from core.models.daily_refresh import DailyRefreshResult
from core.models.daily_refresh_analytics import DailyRefreshSummary


def summarize_daily_refreshes(
    results: list[DailyRefreshResult],
    *,
    generated_at: datetime,
) -> DailyRefreshSummary:
    """Summarize reliability across recorded daily-refresh outcomes."""
    completed_count = sum(
        result.status == "COMPLETED"
        for result in results
    )
    completed_with_warnings_count = sum(
        result.status == "COMPLETED_WITH_WARNINGS"
        for result in results
    )
    failed_count = sum(
        result.status == "FAILED"
        for result in results
    )

    successful_results = [
        result
        for result in results
        if result.status in {"COMPLETED", "COMPLETED_WITH_WARNINGS"}
    ]

    total_refreshes = len(results)
    successful_count = len(successful_results)

    return DailyRefreshSummary(
        total_refreshes=total_refreshes,
        completed_count=completed_count,
        completed_with_warnings_count=completed_with_warnings_count,
        failed_count=failed_count,
        success_rate=(
            Decimal(successful_count) / Decimal(total_refreshes)
            if total_refreshes > 0
            else Decimal(0)
        ),
        latest_successful_finished_at=(
            max(result.finished_at for result in successful_results)
            if successful_results
            else None
        ),
        generated_at=generated_at,
    )