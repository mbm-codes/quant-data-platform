from pyspark.sql import functions as sf, Window

def get_duplicate_and_kept_records(df, partition_by_cols, order_by_cols):
    w = Window.partitionBy(partition_by_cols)\
                    .orderBy(order_by_cols)
    df_with_rn = df.withColumn("rn", sf.row_number().over(w))
    kept = df_with_rn.filter(sf.col("rn")== 1).drop("rn")
    dropped = df_with_rn.filter(sf.col("rn") > 1).drop("rn")

    return kept, dropped