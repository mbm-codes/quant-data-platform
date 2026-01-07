from lakehouse.iceberg.spark_session import SparkSessionBuilder
from pyspark.sql import functions as sf
from datetime import timezone, datetime





def create_batches(dates, chunk_size):
    i = 0
    while i < len(dates):
        yield dates[i:min(i + chunk_size, len(dates))]
        i += chunk_size

spark = SparkSessionBuilder().get_spark()

#use nse/sample/*csv.gz for sample loads
data_path = "/Users/mihirmehta/Desktop/Study/projects/quant-data-platform/data_sources/nse/hist_files/*.csv.gz"

process_ts = datetime.now(timezone.utc)

process_dt  = datetime.now(timezone.utc).date()
run_id = "1"
job_name = "job_nse_historical_load"
process_id = "pr_nse_historical_load"
pipeline_version = "1.0"
is_backfill = True


df = spark.read.options(header=True).csv(data_path)


df = df.withColumn(
    "symbol",
    sf.regexp_extract(sf.input_file_name(), r"([^/]+)\.csv\.gz$", 1)
)

cols = ["symbol"] + [c for c in df.columns if c != "symbol"]
df = df.select(*cols)
df.show(5, truncate=False)


df = df.withColumn("open", sf.col("open").cast("double"))
df = df.withColumnRenamed("open", "open_price")

df = df.withColumn("high", sf.col("high").cast("double"))
df = df.withColumnRenamed("high", "high_price")

df = df.withColumn("low", sf.col("low").cast("double"))
df = df.withColumnRenamed("low", "low_price")

df = df.withColumn("close", sf.col("close").cast("double"))
df = df.withColumnRenamed("close", "close_price")

df = df.withColumn("date", sf.to_date(sf.col("date")))
df = df.withColumnRenamed("date", "trade_date")


df = df.withColumn("volume", sf.col("volume").cast("bigint"))
df = df.withColumnRenamed("volume", "volume_qty")

df = df.withColumn("dividends", sf.col("dividends").cast("double"))

df = df.withColumn("Stock Splits", sf.col("Stock Splits").cast("double"))
df = df.withColumnRenamed("Stock Splits", "stock_splits")



df = df.withColumn("exchange", sf.lit("NSE"))
df = df.withColumn("data_src", sf.lit("kaggle_historical"))

df = df.withColumn("source_file", sf.input_file_name())
df = df.withColumn("ingestion_ts", sf.lit(process_ts))
df = df.withColumn("execution_date", sf.lit(process_dt))
df = df.withColumn("run_id", sf.lit(run_id))
df = df.withColumn("job_name", sf.lit(job_name))
df = df.withColumn("process_id", sf.lit(process_id))
df = df.withColumn("pipeline_version", sf.lit(pipeline_version))

df = df.withColumn("is_backfill", sf.lit(is_backfill))

df = df.withColumn("trade_year", sf.year("trade_date"))
df = df.withColumn("trade_month", sf.month("trade_date"))
df = df.withColumn("trade_day", sf.dayofmonth("trade_date"))

df.show(1, truncate=False) 





spark.sql("DELETE FROM local.market_lakehouse.bronze_nse_historical_prices_raw ")
spark.sql("CALL local.system.expire_snapshots( table => 'local.market_lakehouse.bronze_nse_historical_prices_raw', older_than => TIMESTAMP '2026-01-01')")
spark.sql("CALL local.system.remove_orphan_files(table => 'local.market_lakehouse.bronze_nse_historical_prices_raw')")


dates = sorted([row.trade_date for row in df.select("trade_date").distinct().collect()])
print(datetime.now())
chunk_size = 2000
for batch in create_batches(dates, chunk_size):
    (
        df.filter(sf.col("trade_date").isin(batch))
          #.repartition("trade_date")
          .coalesce(4)
          .write
          .mode("append")
          .insertInto("local.market_lakehouse.bronze_nse_historical_prices_raw")
    )

spark.sql("CALL local.system.rewrite_data_files( table => 'local.market_lakehouse.bronze_nse_historical_prices_raw')")

print(datetime.now())
spark.sql("select * from local.market_lakehouse.bronze_nse_historical_prices_raw limit 5").show(truncate=False)

spark.sql("select count(1) from local.market_lakehouse.bronze_nse_historical_prices_raw").show(truncate=False)





#spark.sql("select replace('RELIANCE.NS.csv.gz', '.csv.gz', '')").show()

# How These Columns Are Populated (Practically)
    # Airflow generates:
        # run_id
        # execution_date

    # Spark injects:
        # process_id

    # CI/CD or Git injects:
        # pipeline_version

    # Job config injects:
        # job_name
        # is_backfill



