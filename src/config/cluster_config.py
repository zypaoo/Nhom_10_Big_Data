import os
import socket
import sys

# Tự động nhận diện nếu tiến trình đang chạy là tính năng realtime streaming
cmd_line = " ".join(sys.argv).lower()
is_streaming = any(
    x in cmd_line 
    for x in ["streaming", "profit_stream", "dashboard", "app.py", "order_producer", "run_all"]
)

# Chỉ tính năng streaming mới hỗ trợ chuyển đổi chế độ qua RUN_MODE env var.
# Mặc định tất cả các tác vụ khác (ML notebooks, SQL queries) luôn là cluster mode.
if is_streaming:
    MODE = os.environ.get("RUN_MODE", "cluster").lower()
else:
    MODE = "cluster"

def get_local_ip():
    try:
        # Kết nối tới IP Master của Radmin VPN để tự động phát hiện card mạng VPN
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("26.97.56.101", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"

if MODE == "cluster":
    SPARK_MASTER = os.environ.get("SPARK_MASTER", "spark://master:7077")
    DRIVER_IP = os.environ.get("DRIVER_IP", get_local_ip())
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
    
    # Chế độ local: Sử dụng tối đa tài nguyên máy local (không giới hạn) để chạy mượt
    EXECUTOR_MEMORY = os.environ.get("EXECUTOR_MEMORY", "8g")
    EXECUTOR_CORES = int(os.environ.get("EXECUTOR_CORES", "8"))
    MAX_CORES = int(os.environ.get("MAX_CORES", "16"))

# Partition Config
DEFAULT_SHUFFLE_PARTITIONS = int(os.environ.get("DEFAULT_SHUFFLE_PARTITIONS", "8"))
DEFAULT_PARALLELISM = int(os.environ.get("DEFAULT_PARALLELISM", "8"))

APP_NAME = os.environ.get("APP_NAME", "Nhom10_BigData")

# Kafka Config
KAFKA_HOME = os.environ.get("KAFKA_HOME", r"D:\java\kafka_2.13-4.1.2")
INPUT_TOPIC = os.environ.get("INPUT_TOPIC", "profit_prediction_requests")
OUTPUT_TOPIC = os.environ.get("OUTPUT_TOPIC", "profit_prediction_results")
