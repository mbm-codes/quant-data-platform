
def safe_count(df, logger=None):
    try:
        return df.count()
    except Exception as e:
        if logger:
            logger.warning(f"Error counting records: {e}")
        return None
