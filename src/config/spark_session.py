import os
os.environ["HADOOP_USER_NAME"] = "vudua"

from pyspark.sql import SparkSession
from src.config.cluster_config import (
    SPARK_MASTER,
    DRIVER_IP,
    EXECUTOR_MEMORY,
    EXECUTOR_CORES,
    MAX_CORES,
    DEFAULT_SHUFFLE_PARTITIONS,
    DEFAULT_PARALLELISM,
    APP_NAME
)
def get_spark(app_name=APP_NAME):
    return (
        SparkSession.builder
        .appName(app_name)
        .master(SPARK_MASTER)
        .config("spark.executor.memory", EXECUTOR_MEMORY)
        .config("spark.executor.cores", EXECUTOR_CORES)
        .config("spark.cores.max", MAX_CORES)
        .config("spark.sql.shuffle.partitions",DEFAULT_SHUFFLE_PARTITIONS)
        .config("spark.default.parallelism",DEFAULT_PARALLELISM)
        .config("spark.driver.host", DRIVER_IP)
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.dynamicAllocation.enabled", "false")
        .getOrCreate()
    )


