from core.quality.file_checks import FileExistsCheck, NonEmptyFileCheck

def build_file_checks(spark, cfg: dict, file_path: str):

    checks = []

    for check in cfg:
        if not check.get("enabled", True):
            continue

        name = check["name"]

        if name == "file_exists":
            checks.append(FileExistsCheck(file_path))
        elif name == "non_empty_file":
            checks.append(
                NonEmptyFileCheck(
                    file_path=file_path,
                    spark=spark,
                    min_rows=check.get("min_rows", 1)
                )
            )
        else:
            raise ValueError(f"Unknown file check: {name}")
    
    return checks