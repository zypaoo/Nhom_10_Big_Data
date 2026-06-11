# src/config/hdfs_config.py

HDFS_HOST = "master"
HDFS_PORT = "9000"

BASE_PATH = f"hdfs://{HDFS_HOST}:{HDFS_PORT}"

SUPERSTORE_DATASET = (
    f"{BASE_PATH}/bigdata/superstore/input/G10_dataset.csv"
)
