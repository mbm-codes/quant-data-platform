from core.quality.enums import DQAction
from pyspark.sql.functions import current_timestamp, lit, to_json, struct
from core.quality.enums import DQAction
from core.quality.results import CheckResult

def write_quarantine_records(
        quarantined_df,
        check_result: CheckResult,
        run_id: str,
        dataset_name: str,
        spark,
        curr_ts,
        sample_size: int = 5

):
    """
    Writes a single quarantine audit record for a DQ check.
    Does not mutate source data.
    """
    row_count = quarantined_df.count()
    if row_count == 0:
        return
    
    sample_rows = (
        quarantined_df
        .limit(sample_size)
        .select(
            to_json(struct("*")).alias("row_json")
        )
        .collect()
    )

    sample_payload = [
        {"row": r["row_json"]}
        for r in sample_rows
    ]

    quarantine_record = spark.createDataFrame(
        [{
            "run_id": run_id,
            "dataset_name": dataset_name,
            "check_name": check_result.check_name,
            "dq_status": check_result.status.value,
            "dq_action": check_result.action.value,
            "reason": check_result.message,
            "invalid_predicate": check_result.invalid_predicate,
            "row_count": row_count,
            "sample_rows": sample_payload,
            "created_at": curr_ts
        }]
    )

    (
        quarantine_record
        .write
        .format("iceberg")
        .mode("append")
        .save("local.market_lakehouse.quarantine_records")
    )

def apply_actions(spark, ctx, df, check_results, run_id, dataset_name ):
    quarantine_predicates = []

    #1 Block 
    for r in check_results:
        if r.action == DQAction.BLOCK:
            raise RuntimeError(
                f"Pipeline blocked by {r.check_name}: {r.message}"
            )

    #2 Collect quarantine / clean predicates
    for r in check_results:
        if r.action in {DQAction.QUARANTINE, DQAction.CLEAN}:
            if r.invalid_predicate:
                quarantine_predicates.append((r, r.invalid_predicate))
    
    #3 Write quarantine records
    for result, predicate in quarantine_predicates:
        if result.check_name.lower() == 'uniquenesscheck':
            df.createOrReplaceTempView("t")
            query = f"""
                SELECT *
                FROM t
                WHERE (
                    {predicate}
                ) IN (
                    SELECT {predicate}
                    FROM t 
                    GROUP BY {predicate}
                    HAVING COUNT(1) > 1
                )
                
            """
            
            quarantined_df = spark.sql(query)
        else:
            quarantined_df = df.filter(predicate)
        write_quarantine_records(
            quarantined_df,
            result,
            run_id,
            dataset_name,
            spark, 
            ctx.process.process_timestamp

        )
    
    #4 Apply CLEAN 
    clean_predicates = [
        pred for r, pred in quarantine_predicates
        if r.action == DQAction.CLEAN
    ]

    for predicate in clean_predicates:
        #df = df.filter(f"{pred}")
        orig_df = df
        df.createOrReplaceTempView("t")
        query = f"""
            SELECT *
            FROM t
            WHERE (
                {predicate}
            ) IN (
                SELECT {predicate}
                FROM t 
                GROUP BY {predicate}
                HAVING COUNT(1) > 1
            )
            
        """

        df = spark.sql(query)

        col_list = [col.strip() for col in predicate.replace('`', '').split(",") ]
        return orig_df.dropDuplicates(col_list)

    