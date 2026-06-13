#!/bin/bash
# ==============================================================================
# Script khởi chạy Spark Structured Streaming Job trên cụm Spark Master
# Gói nạp (packages) là spark-sql-kafka để liên kết Spark và Kafka Broker
# ==============================================================================

# Xác định đường dẫn thư mục dự án
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

echo "------------------------------------------------------------"
echo "🚀 ĐANG KHỞI CHẠY SPARK REALTIME STREAMING JOB..."
echo "------------------------------------------------------------"

# Spark Submit lên Master Node (Cần bật Hadoop DFS và Kafka Broker trước)
# Sử dụng gói maven org.apache.spark:spark-sql-kafka-0-10 phù hợp với Spark 3.x
spark-submit \
  --master spark://master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --driver-memory 2g \
  --executor-memory 2g \
  --executor-cores 2 \
  --conf spark.sql.shuffle.partitions=2 \
  "$PROJECT_DIR/src/streaming/profit_stream.py"
