# src/config/cluster_config.py
import os

# Nhận diện chế độ chạy (mặc định là local)
MODE = os.environ.get("RUN_MODE", "local").lower()

if MODE == "cluster":
    SPARK_MASTER = os.environ.get("SPARK_MASTER", "spark://master:7077")
    DRIVER_IP = os.environ.get("DRIVER_IP", "worker1")
    KAFKA_BOOTSTRAP_SERVER = os.environ.get("KAFKA_BOOTSTRAP_SERVER", "master:9092")
    CHECKPOINT_PATH = os.environ.get("CHECKPOINT_PATH", "hdfs://master:9000/checkpoints/profit_stream")
    PREDICTIONS_OUTPUT_PATH = os.environ.get("PREDICTIONS_OUTPUT_PATH", "hdfs://master:9000/bigdata/realtime/profit_predictions/")
    
    EXECUTOR_MEMORY = os.environ.get("EXECUTOR_MEMORY", "3g")
    EXECUTOR_CORES = int(os.environ.get("EXECUTOR_CORES", "3"))
    MAX_CORES = int(os.environ.get("MAX_CORES", "12"))
else:
    SPARK_MASTER = os.environ.get("SPARK_MASTER", "local[*]")
    DRIVER_IP = os.environ.get("DRIVER_IP", "localhost")
    KAFKA_BOOTSTRAP_SERVER = os.environ.get("KAFKA_BOOTSTRAP_SERVER", "localhost:9092")
    CHECKPOINT_PATH = os.environ.get("CHECKPOINT_PATH", "src/streaming/checkpoints/profit_stream")
    PREDICTIONS_OUTPUT_PATH = os.environ.get("PREDICTIONS_OUTPUT_PATH", "src/streaming/realtime_predictions_parquet")
    
    EXECUTOR_MEMORY = os.environ.get("EXECUTOR_MEMORY", "1g")
    EXECUTOR_CORES = int(os.environ.get("EXECUTOR_CORES", "1"))
    MAX_CORES = int(os.environ.get("MAX_CORES", "4"))

# Partition Config
DEFAULT_SHUFFLE_PARTITIONS = int(os.environ.get("DEFAULT_SHUFFLE_PARTITIONS", "8"))
DEFAULT_PARALLELISM = int(os.environ.get("DEFAULT_PARALLELISM", "8"))

APP_NAME = os.environ.get("APP_NAME", "Nhom10_BigData")

# Kafka Config
KAFKA_HOME = os.environ.get("KAFKA_HOME", r"D:\java\kafka_2.13-4.1.2")
INPUT_TOPIC = os.environ.get("INPUT_TOPIC", "profit_prediction_requests")
OUTPUT_TOPIC = os.environ.get("OUTPUT_TOPIC", "profit_prediction_results")
