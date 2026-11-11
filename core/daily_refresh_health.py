from datetime import datetime

from core.models.daily_refresh import DailyRefreshResult
from core.models.daily_refresh_health import DailyRefreshHealth


def assess_daily_refresh_health(
    results: list[DailyRefreshResult],
    *,
    max_consecutive_failures: int,
    checked_at: datetime,
) -> DailyRefreshHealth:
    """Derive an actionable health state from refresh outcome history."""
    if not results:
        return DailyRefreshHealth(
            status="MISSING",
            consecutive_failure_count=0,
            max_consecutive_failures=max_consecutive_failures,
            reason="No daily refresh results exist yet.",
            checked_at=checked_at,
        )

    ordered_results = sorted(
        results,
        key=lambda result: (result.finished_at, result.run_id),
        reverse=True,
    )
    latest_result = ordered_results[0]

    consecutive_failure_count = 0
    for result in ordered_results:
        if result.status != "FAILED":
            break

        consecutive_failure_count += 1

    if consecutive_failure_count >= max_consecutive_failures:
        status = "CRITICAL"
        reason = (
            "Consecutive daily-refresh failure limit reached: "
            f"{consecutive_failure_count}."
        )
    elif latest_result.status == "FAILED":
        status = "WARNING"
        reason = "Latest daily refresh failed."
    elif latest_result.status == "COMPLETED_WITH_WARNINGS":
        status = "WARNING"
        reason = "Latest daily refresh completed with warnings."
    else:
        status = "HEALTHY"
        reason = "Latest daily refresh completed successfully."

    return DailyRefreshHealth(
        status=status,
        latest_run_id=latest_result.run_id,
        latest_refresh_status=latest_result.status,
        latest_finished_at=latest_result.finished_at,
        consecutive_failure_count=consecutive_failure_count,
        max_consecutive_failures=max_consecutive_failures,
        reason=reason,
        checked_at=checked_at,
    )