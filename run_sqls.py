from lakehouse.iceberg.spark_session import SparkSessionBuilder


spark = SparkSessionBuilder().get_spark()

spark.sql("select count(1) from local.market_lakehouse.bronze_nse_historical_prices_raw").show(truncate=False)
spark.sql("select symbol,count(1), min(trade_date) as min_dt, max(trade_date) as max_dt from local.market_lakehouse.bronze_nse_historical_prices_raw group by symbol order by 1").show(truncate=False)
spark.sql("select trade_date, count(1) from local.market_lakehouse.bronze_nse_historical_prices_raw group by trade_date order by 1").show(truncate=False)
spark.sql("select count(1) from local.market_lakehouse.bronze_nse_historical_prices_raw").show()

