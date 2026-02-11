import os
import glob
from typing import List
from core.quality.base import DataQualityCheck, CheckResult
from core.quality.enums import CheckStatus, DQAction


class FileExistsCheck(DataQualityCheck):
    """
    Validates that one or more files exist.

    Supports:
    - Exact file paths
    - Glob patterns (e.g. /data/*.csv.gz)
    """

    def __init__(
        self,
        path: str,
        min_files: int = 1,
        allow_glob: bool = True,
    ):
        self.path = path
        self.min_files = min_files
        self.allow_glob = allow_glob

    def _resolve_files(self) -> List[str]:
        if self.allow_glob and any(char in self.path for char in ["*", "?", "["]):
            return [f for f in glob.glob(self.path) if os.path.isfile(f)]
        return [self.path] if os.path.isfile(self.path) else []

    def run(self, context=None) -> CheckResult:
        files = self._resolve_files()
        file_count = len(files)

        if file_count >= self.min_files:
            return CheckResult(
                check_name="FileExistsCheck",
                status=CheckStatus.PASS,
                action=DQAction.OBSERVE,
                message=f"Found {file_count} file(s) at path: {self.path}",
                metrics={"file_count": file_count}
            )

        return CheckResult(
            check_name="FileExistsCheck",
            status=CheckStatus.FAIL,
            action=DQAction.OBSERVE,
            message=(
                f"Found {file_count} file(s) "
                f"(min required: {self.min_files}) at path: {self.path}"
            ),
            metrics={"file_count": file_count}
        )

class NonEmptyFileCheck(DataQualityCheck):
    def __init__(self, file_path: str, spark, min_rows: int = 1):
        self.file_path = file_path
        self.spark = spark
        self.min_rows = min_rows

    def run(self, context=None) -> CheckResult:
        try:
            df = self.spark.read.csv(self.file_path, header=True)
            row_count = df.count()

            if row_count >= self.min_rows:
                return CheckResult(
                    check_name="NonEmptyFileCheck",
                    status=CheckStatus.PASS,
                    action=DQAction.OBSERVE,
                    message=f"File has {row_count} rows",
                    metrics={"row_count": row_count}
                )
            
            return CheckResult(
                check_name="NonEmptyFileCheck",
                status=CheckStatus.FAIL,
                action=DQAction.BLOCK,
                message=f"File has only {row_count} rows (min required: {self.min_rows})",
                metrics={"row_count": row_count}

            )
        
        except Exception as e:
            return CheckResult(
                check_name="NonEmptyFileCheck",
                status=CheckStatus.FAIL,
                action=DQAction.OBSERVE,
                message=f"Failed to read file: {str(e)}"
            )