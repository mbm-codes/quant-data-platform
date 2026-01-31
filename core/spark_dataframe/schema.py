from pyspark.sql import functions as sf


def enforce_schema(df, schema, logger=None, strict=True, stage="unknown"):
        """
        Enforces a schema contract.

        strict=True:
        - missing columns -> error
        - extra columns -> error

        strict=False:
        - missing columns -> add as null
        - extra columns -> drop
        """

        expected_cols = {field.name for field in schema}
        actual_cols = set(df.columns)
        extra_cols = actual_cols - expected_cols
        missing_cols = expected_cols - actual_cols

        missing_cols_added = []

        if strict:
            errors = []
            if missing_cols:
                errors.append(f"[{stage}] Missing columns: {sorted(missing_cols)}")
            if extra_cols:
                errors.append(f"[{stage}] Extra columns: {sorted(extra_cols)}")
            if errors:
                msg = f"[{stage}] Schema validation failed: " + "; ".join(errors)
                if logger:
                    logger.error(msg)
                raise ValueError(msg)

            # Column order enforcement
            return df.select([f.name for f in schema]), missing_cols_added
        # ---------------------------
        # STRICT MODE: FALSE
        # ---------------------------

        for field in missing_cols:
            missing_cols_added.append(field)
            df = df.withColumn(field, sf.lit(None).cast(schema[field].dataType))
            if logger:
                logger.warning(f"[{stage}] Missing column added as null: {field}")
        
        if extra_cols:
            if logger:
                logger.warning(f"[{stage}] Extra columns dropped: {sorted(extra_cols)}")
            df = df.drop(*extra_cols)
        
        return df.select([f.name for f in schema]), missing_cols_added