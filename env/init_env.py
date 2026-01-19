
from pyspark.sql import SparkSession
from lakehouse.iceberg.spark_session import SparkSessionBuilder
import os
import subprocess





# ---- CONFIG ----
WAREHOUSE_PATH = "/Users/mihirmehta/Desktop/Study/projects/quant-data-platform/warehouse"
CATALOG_NAME = "local"
APP_NAME = "qdp-env-init"

spark = SparkSessionBuilder(
    app_name="qdp-local",
    environment="local",
).get_spark()


os.makedirs(WAREHOUSE_PATH, exist_ok=True)


def run_sql_trino(file_path):

    sql = open(file_path).read()
    sql = sql.replace("CREATE DATABASE", "CREATE SCHEMA")
    sql = sql.replace("USE DATABASE", "USE")

    subprocess.run([
        "docker", "exec", "-i", "trino",
        "trino",
        "--server", "http://localhost:8080",
        "--catalog", "local",          # adjust if needed
        "--schema", "default",        # adjust if needed
        "--execute", sql,
    ], check=True)



#--------------------------


def run_sql_spark(file_path):
    
   

    print("Available catalogs:")
    #spark.sql("SHOW CATALOGS").show(truncate=False)

    print("Available namespaces in local catalog:")
    #spark.sql(f"SHOW NAMESPACES IN {CATALOG_NAME}").show(truncate=False)

    # ---- OPTIONAL: CREATE DEFAULT NAMESPACE ----
    #spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG_NAME}.default")

    print("Iceberg environment initialized successfully.")

    with open(file_path) as f:
        sql_text = f.read()

    for stmt in sql_text.split(";"):
        stmt = stmt.strip()
        print(stmt)
        if not stmt:
            continue
        try:
            spark.sql(stmt)
        except Exception as e:
            print(f"Failed SQL: \n {stmt}")
            raise e




run_sql_spark("env/databases.sql")
run_sql_spark("env/tables/bronze.sql")

spark.sql("select count(1) from local.market_lakehouse.bronze_nse_historical_prices_raw").show()


# run_sql_spark("env/tables/silver.sql")
# run_sql_spark("env/tables/gold.sql")
# run_sql_spark("env/master_data/trading_calendar.sql")