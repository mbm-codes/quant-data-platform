from pyspark.sql import functions as sf

def normalize_column_names(df):
    return df.select([sf.col(c).alias(c.strip().lower().replace(" ", "_")) for c in df.columns])

def standardize_date(df, col_names, src_format):

    for c in col_names:
        df = df.withColumn(
            c,
            sf.to_date(sf.trim(sf.col(c)), src_format)
        )

    return df

def cast_and_rename_columns(df, cast_rename_config ):
    for src_col, (dst_col, dtype) in cast_rename_config.items():
            if src_col in df.columns:
                if src_col == dst_col:
                     df = df.withColumn(dst_col, sf.col(src_col).cast(dtype))
                else:
                    df = df.withColumn(dst_col, sf.col(src_col).cast(dtype)).drop(src_col)
    return df