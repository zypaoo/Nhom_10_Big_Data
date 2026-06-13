# src/config/cluster_config.py

SPARK_MASTER = "spark://master:7077"

DRIVER_IP = "worker1"

EXECUTOR_MEMORY = "3g"
EXECUTOR_CORES = 3
MAX_CORES = 12

# Partition Config
DEFAULT_SHUFFLE_PARTITIONS = 8
DEFAULT_PARALLELISM = 8

APP_NAME = "Nhom10_BigData"

# Kafka Config
KAFKA_HOME = r"D:\java\kafka_2.13-4.1.2"
KAFKA_BOOTSTRAP_SERVER = "master:9092"
INPUT_TOPIC = "profit_prediction_requests"
OUTPUT_TOPIC = "profit_prediction_results"


# Streaming Checkpoint
CHECKPOINT_PATH = "hdfs://master:9000/checkpoints/profit_stream"