from pyspark.sql import functions as sf, Row
from core.spark_dataframe.transforms import cast_and_rename_columns
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, FloatType, DoubleType, DateType, TimestampType, BooleanType, DecimalType, ArrayType, MapType
from datetime import datetime
from decimal import Decimal



def test_only_cast_column(spark):

    raw_data = [
        Row(
            string_col="hello",
            int_col="42",
            long_col="12345678901",
            double_col="10.75",
            float_col="5.5",
            boolean_col="true",
            date_col="2024-01-15",
            timestamp_col="2024-01-15 10:30:45",
            decimal_col="1234.56",
            array_col='["a","b","c"]',
            map_col='{"x":1,"y":2}',
            struct_col='{"nested_str":"inner","nested_int":10}'
        )
    ]

    df_raw = spark.createDataFrame(raw_data)
    CAST_RENAME_CONFIG = {
        # primitives
        "string_col": ("string_col", "string"),
        "int_col": ("int_col", "int"),
        "long_col": ("long_col", "bigint"),
        "double_col": ("double_col", "double"),
        "float_col": ("float_col", "float"),
        "boolean_col": ("boolean_col", "boolean"),

        # temporal
        "date_col": ("date_col", "date"),
        "timestamp_col": ("timestamp_col", "timestamp"),

        # numeric precision
        "decimal_col": ("decimal_col", "decimal(10,2)"),

        # complex types (JSON strings → Spark types)
        "array_col": ("array_col", "array<string>"),
        "map_col": ("map_col", "map<string,int>"),
        "struct_col": (
            "struct_col",
            "struct<nested_str:string,nested_int:int>"
        ),
    }

    casted_df = cast_and_rename_columns(df_raw, CAST_RENAME_CONFIG)
    
    result = casted_df.collect()
    
    assert result[0]["string_col"] == "hello"
    assert result[0]["int_col"] == 42
    assert result[0]["long_col"] == 12345678901
    assert result[0]["double_col"] == 10.75
    assert result[0]["float_col"] == 5.5
    assert result[0]["boolean_col"] == True
    assert result[0]["date_col"] == datetime.strptime("2024-01-15", "%Y-%m-%d").date()
    assert result[0]["timestamp_col"] == datetime.strptime("2024-01-15 10:30:45", "%Y-%m-%d %H:%M:%S")
    assert result[0]["decimal_col"] == Decimal("1234.56")
    assert result[0]["array_col"] == ["a","b","c"]
    assert result[0]["map_col"] == {"x":1,"y":2}
    struct_val = result[0]["struct_col"] 
    assert struct_val.nested_str == "inner"
    assert struct_val.nested_int == 10
    

def test_only_rename_column(spark):
    
    struct_val = Row(
        nested_str="inner",
        nested_int=10
    )

    # Raw test data
    raw_data = [
        Row(
            string_col="hello",
            int_col=42,
            long_col=12345678901,
            double_col=10.75,
            float_col=5.5,
            boolean_col=True,
            date_col=datetime.strptime("2024-01-15", "%Y-%m-%d").date(),
            timestamp_col=datetime.strptime("2024-01-15 10:30:45", "%Y-%m-%d %H:%M:%S"),
            decimal_col=Decimal("1234.56"),
            array_col=["a","b","c"],
            map_col={"x":1, "y":2},
            struct_col=struct_val
        )
    ]

    # Explicit schema (optional, but recommended for precision/scale)
    schema = StructType([
        StructField("string_col", StringType(), True),
        StructField("int_col", IntegerType(), True),
        StructField("long_col", LongType(), True),
        StructField("double_col", DoubleType(), True),
        StructField("float_col", FloatType(), True),
        StructField("boolean_col", BooleanType(), True),
        StructField("date_col", DateType(), True),
        StructField("timestamp_col", TimestampType(), True),
        StructField("decimal_col", DecimalType(10, 2), True),
        StructField("array_col", ArrayType(StringType()), True),
        StructField("map_col", MapType(StringType(), IntegerType()), True),
        StructField("struct_col", StructType([
            StructField("nested_str", StringType(), True),
            StructField("nested_int", IntegerType(), True)
        ]), True)
    ])

    # Create DataFrame
    df_input = spark.createDataFrame(raw_data, schema=schema)

    RENAME_CONFIG = {
        # primitives
        "string_col": ("string_col1", "string"),
        "int_col": ("int_col1", "int"),
        "long_col": ("long_col1", "bigint"),
        "double_col": ("double_col1", "double"),
        "float_col": ("float_col1", "float"),
        "boolean_col": ("boolean_col1", "boolean"),

        # temporal
        "date_col": ("date_col1", "date"),
        "timestamp_col": ("timestamp_col1", "timestamp"),

        # numeric precision
        "decimal_col": ("decimal_col1", "decimal(10,2)"),

        # complex types (JSON strings → Spark types)
        "array_col": ("array_col1", "array<string>"),
        "map_col": ("map_col1", "map<string,int>"),
        "struct_col": (
            "struct_col1",
            "struct<nested_str:string,nested_int:int>"
        ),
    }

    renamed_df = cast_and_rename_columns(df_input, RENAME_CONFIG)
    expected_col_list = ["string_col1","int_col1","long_col1","double_col1","float_col1","boolean_col1","date_col1","timestamp_col1","decimal_col1","array_col1","map_col1","struct_col1"]
    
    assert sorted(expected_col_list) == sorted(renamed_df.columns)   # type: ignore


def test_cast_and_rename_columns(spark):
    raw_data = [
        Row(
            string_col="hello",
            int_col="42",
            long_col="12345678901",
            double_col="10.75",
            float_col="5.5",
            boolean_col="true",
            date_col="2024-01-15",
            timestamp_col="2024-01-15 10:30:45",
            decimal_col="1234.56",
            array_col='["a","b","c"]',
            map_col='{"x":1,"y":2}',
            struct_col='{"nested_str":"inner","nested_int":10}'
        )
    ]

    df_raw = spark.createDataFrame(raw_data)
    CAST_RENAME_CONFIG = {
        # primitives
        "string_col": ("string_col2", "string"),
        "int_col": ("int_col2", "int"),
        "long_col": ("long_col2", "bigint"),
        "double_col": ("double_col2", "double"),
        "float_col": ("float_col2", "float"),
        "boolean_col": ("boolean_col2", "boolean"),

        # temporal
        "date_col": ("date_col2", "date"),
        "timestamp_col": ("timestamp_col2", "timestamp"),

        # numeric precision
        "decimal_col": ("decimal_col2", "decimal(10,2)"),

        # complex types (JSON strings → Spark types)
        "array_col": ("array_col2", "array<string>"),
        "map_col": ("map_col2", "map<string,int>"),
        "struct_col": (
            "struct_col2",
            "struct<nested_str:string,nested_int:int>"
        ),
    }

    cast_renamed_df = cast_and_rename_columns(df_raw, CAST_RENAME_CONFIG)
    
    result = cast_renamed_df.collect()
    
    assert result[0]["string_col2"] == "hello"
    assert result[0]["int_col2"] == 42
    assert result[0]["long_col2"] == 12345678901
    assert result[0]["double_col2"] == 10.75
    assert result[0]["float_col2"] == 5.5
    assert result[0]["boolean_col2"] == True
    assert result[0]["date_col2"] == datetime.strptime("2024-01-15", "%Y-%m-%d").date()
    assert result[0]["timestamp_col2"] == datetime.strptime("2024-01-15 10:30:45", "%Y-%m-%d %H:%M:%S")
    assert result[0]["decimal_col2"] == Decimal("1234.56")
    assert result[0]["array_col2"] == ["a","b","c"]
    assert result[0]["map_col2"] == {"x":1,"y":2}
    struct_val = result[0]["struct_col2"] 
    assert struct_val.nested_str == "inner"
    assert struct_val.nested_int == 10


    expected_col_list = ["string_col2","int_col2","long_col2","double_col2","float_col2","boolean_col2","date_col2","timestamp_col2","decimal_col2","array_col2","map_col2","struct_col2"]

    
    assert sorted(expected_col_list) == sorted(cast_renamed_df.columns)   # type: ignore
    


   





