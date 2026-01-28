from pyspark.sql import functions as sf
from pyspark.sql.types import MapType, StringType,IntegerType,DoubleType,DecimalType,LongType,BooleanType,FloatType,DateType,TimestampType, DataType, _parse_datatype_string, StructType, ArrayType

def normalize_column_names(df):
    return df.select([sf.col(c).alias(c.strip().lower().replace(" ", "_")) for c in df.columns])

def standardize_date(df, col_names, src_format):

    for c in col_names:
        df = df.withColumn(
            c,
            sf.to_date(sf.trim(sf.col(c)), src_format)
        )

    return df

def cast_and_rename_columns(df, cast_rename_config: dict):
    """
    Casts and/or renames columns in a DataFrame based on a config dict.
    Supports:
        - Primitive types: int, double, string, etc.
        - Decimal types: decimal(precision,scale)
        - Complex types: array<>, map<>, struct<>, with auto JSON parsing if needed
    """
    def _replace_or_rename(df, src_col, dst_col, expr):
        df = df.withColumn(dst_col, expr)
        if src_col != dst_col:
            df = df.drop(src_col)
        return df

    for src_col, (dst_col, dtype_str) in cast_rename_config.items():
        if src_col not in df.columns:
            continue

        col_data = df.schema[src_col].dataType

        # --- Decimal type ---
        if dtype_str.lower().startswith("decimal"):
            import re
            m = re.match(r"decimal\((\d+),(\d+)\)", dtype_str)
            if m:
                precision, scale = int(m.group(1)), int(m.group(2))
                expr = sf.col(src_col).cast(DecimalType(precision, scale))
            else:
                expr = sf.col(src_col).cast("decimal")

        # --- Complex types ---
        elif dtype_str.lower().startswith(("struct", "array", "map")):
            target_dtype = _parse_datatype_string(dtype_str)

            # Check if the source is already a compatible Spark type
            if isinstance(col_data, (StructType, ArrayType, MapType)):
                # Already Spark type → just rename
                expr = sf.col(src_col)
            else:
                # Assume it's JSON string → parse
                expr = sf.from_json(sf.col(src_col), target_dtype)    # type: ignore

        # --- Primitive types ---
        else:
            expr = sf.col(src_col).cast(dtype_str)

        df = _replace_or_rename(df, src_col, dst_col, expr)

    return df