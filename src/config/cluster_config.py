# src/config/cluster_config.py

SPARK_MASTER = "spark://master:7077"

DRIVER_IP = "master"

EXECUTOR_MEMORY = "3g"
EXECUTOR_CORES = 3
MAX_CORES = 12

# Partition Config
DEFAULT_SHUFFLE_PARTITIONS = 8
DEFAULT_PARALLELISM = 8

APP_NAME = "Nhom10_BigData"