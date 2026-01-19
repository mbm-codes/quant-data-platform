import logging
from logging.handlers import RotatingFileHandler
import os

class QDPLogger:
    """
    QDPLogger supports:
    - Console logging
    - Rotating file logging
    - Separate app.log and spark.log
    - Singleton per logger name
    - Redirect Spark driver (py4j) logs to spark.log
    """
    # Expose logging levels as class attributes
    CRITICAL = logging.CRITICAL
    ERROR = logging.ERROR
    WARNING = logging.WARNING
    INFO = logging.INFO
    DEBUG = logging.DEBUG

    _logger_instances = {}

    def __new__(cls, name="qdp", app_log_file=None, spark_log_file=None,
                level=logging.INFO, max_bytes=50*1024*1024, backup_count=5):
        # Singleton per logger name
        if name not in cls._logger_instances:
            cls._logger_instances[name] = super().__new__(cls)
        return cls._logger_instances[name]

    def __init__(self, name="qdp", app_log_file=None, spark_log_file=None,
                 level=logging.INFO, max_bytes=50*1024*1024, backup_count=5):

        if hasattr(self, "_initialized") and self._initialized:
            return

        # ----- App logger -----
        self.app_logger = logging.getLogger(f"{name}_app")
        self.app_logger.setLevel(logging.DEBUG)
        self.app_logger.propagate = False

        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            "%Y-%m-%d %H:%M:%S"
        )

        # Console handler for app logs
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        self.app_logger.addHandler(console_handler)

        # File handler for app logs
        if app_log_file:
            app_log_file = os.path.abspath(app_log_file)
            os.makedirs(os.path.dirname(app_log_file), exist_ok=True)
            fh = RotatingFileHandler(app_log_file, maxBytes=max_bytes, backupCount=backup_count)
            fh.setLevel(level)
            fh.setFormatter(formatter)
            self.app_logger.addHandler(fh)

        # ----- Spark logger -----
        self.spark_logger = logging.getLogger(f"{name}_spark")
        self.spark_logger.setLevel(logging.DEBUG)
        self.spark_logger.propagate = False

        if spark_log_file:
            spark_log_file = os.path.abspath(spark_log_file)
            os.makedirs(os.path.dirname(spark_log_file), exist_ok=True)
            spark_fh = RotatingFileHandler(spark_log_file, maxBytes=max_bytes, backupCount=backup_count)
            spark_fh.setLevel(level)
            spark_fh.setFormatter(formatter)
            self.spark_logger.addHandler(spark_fh)

        self._initialized = True

    # ----- App log convenience methods -----
    def debug(self, msg):    self.app_logger.debug(msg)
    def info(self, msg):     self.app_logger.info(msg)
    def warning(self, msg):  self.app_logger.warning(msg)
    def error(self, msg):    self.app_logger.error(msg)
    def exception(self, msg): self.app_logger.exception(msg)

    # ----- Spark driver log integration -----
    def attach_spark_logs(self, spark, level=logging.INFO):
        """
        Redirect Spark driver (py4j) logs to the spark_logger file.
        """
        if not hasattr(spark, "sparkContext"):
            raise ValueError("spark must be a SparkSession")

        # Driver log level
        spark.sparkContext.setLogLevel("WARN")

        # Attach handlers to py4j logger
        # Note: Spark driver logs are handled by the 'py4j' logger. These arent executor logs.
        py4j_logger = logging.getLogger("py4j")
        py4j_logger.setLevel(level)
        py4j_logger.propagate = False

        for handler in self.spark_logger.handlers:
            py4j_logger.addHandler(handler)

        self.info("Spark driver logs attached to spark.log")
