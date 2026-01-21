from pyspark.sql import Row
from ingestion.nse.historical.historical_loads import NSEHistoricalDataPipeline, RAW_INPUT_SCHEMA
from ingestion.common.processing_metadata import create_process_context
from common.logging.logging import QDPLogger

import yaml
import gzip
import shutil
import pytest


# spark fixture is defined in tests/conftest.py
# never import a fixture directly; pytest handles it
@pytest.fixture
def pipeline(spark):
    config = load_test_config()
    logger = QDPLogger(name="test_logger", level=QDPLogger.DEBUG)
    return NSEHistoricalDataPipeline(spark, logger, config)


def load_test_config():
    with open("tests/configs/test_nse_config.yaml") as f:
        return yaml.safe_load(f)

def test_transform_renames_and_casts(pipeline, spark):
    
    input_data = [
        Row(date="2022-01-01", open="100.5", high="110.0", low="99.5", close="105.0", volume="1000", dividends="0.0", stock_splits="0"),
        Row(date="2022-01-02", open="106.0", high="112.0", low="104.0", close="108.0", volume="1500", dividends="0.0", stock_splits="0"),
    ]
    input_df = spark.createDataFrame(input_data)

    ctx = create_process_context(
        pipeline_version="v1.0",
        is_backfill=False,
        process_id="test",
        run_id="1",
        process_name="test",
        spark=spark,
        force_new_run_id=False,
        orchestrator_context=None
    )

    transformed_df = pipeline.transform(input_df, ctx)
    result = transformed_df.collect()

    expected_types = {
        "trade_date": "date",
        "open_price": "double",
        "high_price": "double",
        "low_price": "double",
        "close_price": "double",
        "volume_qty": "bigint",
    }

    for col, dtype in expected_types.items():
        assert transformed_df.schema[col].dataType.simpleString() == dtype

    assert result[0]["trade_date"].isoformat() == "2022-01-01"
    assert result[0]["open_price"] == 100.5
    assert result[0]["high_price"] == 110.0
    assert result[0]["low_price"] == 99.5
    assert result[0]["close_price"] == 105.0
    assert result[0]["volume_qty"] == 1000

def test_transform_missing_column_creates_null(pipeline, spark):
    

    input_data = [
        Row(date="2022-01-01"),
    ]
    input_df = spark.createDataFrame(input_data)
    input_df = pipeline.enforce_schema(input_df, RAW_INPUT_SCHEMA, logger=pipeline.logger, strict=False, stage="raw_input")

    ctx = create_process_context(
        pipeline_version="v1.0",
        is_backfill=False,
        process_id="test",
        run_id="1",
        process_name="test",
        spark=spark,
        force_new_run_id=False,
        orchestrator_context=None
    )

    transformed_df = pipeline.transform(input_df, ctx)
    result = transformed_df.first()

    assert result.open_price is None
    assert result.close_price is None

def test_metadata_columns_added(pipeline, spark):
    

    input_data = [
        Row(date="2022-01-01", open="100.5", high="110.0", low="99.5", close="105.0", volume="1000", dividends="0.0", stock_splits="0"),
    ]
    input_df = spark.createDataFrame(input_data)

    ctx = create_process_context(
        pipeline_version="v1.0",
        is_backfill=False,
        process_id="test_process",
        run_id="run_123",
        process_name="test_pipeline",
        spark=spark,
        force_new_run_id=False,
        orchestrator_context=None
    )

    transformed_df = pipeline.transform(input_df, ctx)
    result = transformed_df.collect()

    expected_cols = ["ingestion_ts", "run_id", "process_id", "pipeline_version"]

    for col in expected_cols:
        assert col in transformed_df.columns


def test_extract_raises_on_schema_mismatch(pipeline, tmp_path):
    data = "invalid_header1,invalid_header2\nvalue1,value2\n"
    file = tmp_path / "invalid_test.csv"
    file.write_text(data)

    with open(file, 'rb') as f_in:
        with gzip.open(f"{file}.gz", 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)


    pipeline.config["input_pattern"] = str(file)+".gz"
    
    with pytest.raises(ValueError) as excinfo:
        df = pipeline.extract()
    
    msg = str(excinfo.value)
    assert "Schema validation failed" in msg
    assert "Missing columns" in msg
    assert "Extra columns" in msg