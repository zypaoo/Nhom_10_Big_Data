import os
os.environ["HADOOP_USER_NAME"] = "vudua"
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from src.config.spark_session import get_spark
from src.config.cluster_config import (
    KAFKA_BOOTSTRAP_SERVER, INPUT_TOPIC, OUTPUT_TOPIC, CHECKPOINT_PATH,
    PREDICTIONS_OUTPUT_PATH, APP_NAME, DEFAULT_SHUFFLE_PARTITIONS, MODE
)
from src.streaming.realtime_model.model_loader import load_pipeline_model
from src.streaming.realtime_model.realtime_feature_pipeline import preprocess_realtime_features
from src.streaming.realtime_model.realtime_predictor import predict_realtime_profit

# Import schemas and paths
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Schema định dạng JSON của đơn hàng nhận từ Kafka
order_schema = StructType([
    StructField("Order_ID", StringType(), True),
    StructField("Customer_ID", StringType(), True),
    StructField("Order_Date", StringType(), True),
    StructField("Category", StringType(), True),
    StructField("Sub_Category", StringType(), True),
    StructField("Sales", DoubleType(), True),
    StructField("Discount", DoubleType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("Shipping_Cost", DoubleType(), True),
    StructField("Market", StringType(), True),
    StructField("Publish_Time", DoubleType(), True)
])

def get_output_paths():
    """Trả về đường dẫn lưu checkpoint và prediction output từ file config"""
    return PREDICTIONS_OUTPUT_PATH, CHECKPOINT_PATH

def write_to_destinations(batch_df, batch_id):
    """
    Hàm foreachBatch xử lý ghi đồng thời ra 2 đích:
    1. Ghi lưu trữ Parquet vào HDFS (phục vụ Audit/Batch Analysis).
    2. Đẩy kết quả dạng JSON vào Kafka Topic để Dashboard Streamlit consume trực tiếp.
    """
    if batch_df.count() == 0:
        return
        
    print(f"Processing micro-batch ID: {batch_id} with {batch_df.count()} records.")
    
    # 1. Xác định đường dẫn output và ghi ra Parquet
    parquet_path, _ = get_output_paths()
    batch_df.select(
        "Order_ID", "Customer_ID", "Order_Date", "Category", "Sub_Category",
        "Sales", "Discount", "Quantity", "Shipping_Cost", "Market",
        "cust_hist_sales", "cust_hist_profit", "cust_hist_margin",
        "prediction_usd", "risk_level", "prediction_time"
    ).write.mode("append").parquet(parquet_path)
    
    # 2. Định dạng dữ liệu và gửi kết quả vào Kafka Output Topic
    kafka_output_df = batch_df.select(
        col("Order_ID").alias("order_id"),
        col("Customer_ID").alias("customer_id"),
        col("Category").alias("category"),
        col("Sub_Category").alias("sub_category"),
        col("Sales").alias("sales"),
        col("Discount").alias("discount"),
        col("prediction_usd").alias("prediction_usd"),
        col("risk_level").alias("risk_level"),
        col("prediction_time").alias("prediction_time"),
        col("Publish_Time").alias("publish_time")
    )
    
    # Chuyển đổi toàn bộ dataframe sang chuỗi JSON trong cột 'value' để Kafka nhận diện
    kafka_payload = kafka_output_df.selectExpr("CAST(order_id AS STRING) AS key", "to_json(struct(*)) AS value")
    
    try:
        kafka_payload.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER) \
            .option("topic", OUTPUT_TOPIC) \
            .save()
        print(f"Successfully published batch {batch_id} predictions to Kafka: {OUTPUT_TOPIC}")
    except Exception as e:
        print(f"Error publishing batch {batch_id} to Kafka: {e}")

def main():
    print("=== STARTING SPARK STRUCTURED STREAMING FOR REALTIME PROFIT ===")
    
    # Kiem tra suc khoe cac Node trong cum khi khoi dong de canh bao offline
    if MODE == "cluster":
        print("=== CHECKING CLUSTER NODE HEALTH STATUS ===")
        import socket
        nodes_to_check = {
            "master (HDFS NameNode)": ("26.97.56.101", 9000),
            "worker1 (Windows Client)": ("26.105.196.249", 9866),
            "worker2 (Peer node)": ("26.155.115.30", 9866)
        }
        for name, (ip, port) in nodes_to_check.items():
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(1.5)
                s.connect((ip, port))
                print(f"Node {name}: ONLINE")
                s.close()
            except Exception:
                print(f"Node {name}: OFFLINE [WARNING: Se tu dong bo qua va su dung cac replica tren node khac de dung loi]")
        print("============================================")
    
    # Khởi tạo Spark Session từ cấu hình chuẩn
    spark = get_spark(APP_NAME)
        
    # Tải bảng Customer Snapshot để phục vụ phép Join (đã chuyển vào thư mục streaming)
    hdfs_path = "hdfs://master:9000/bigdata/ml2/customer_profile.parquet"
    local_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "customer_profile.parquet"))
    alternative_path = os.path.abspath(os.path.join(os.getcwd(), "src", "streaming", "customer_profile.parquet"))
    
    if MODE == "cluster":
        snapshot_path = hdfs_path
    else:
        if os.path.exists(local_path):
            snapshot_path = local_path
        elif os.path.exists(alternative_path):
            snapshot_path = alternative_path
        else:
            print(f"Warning: Customer snapshot path not found locally (checked '{local_path}' and '{alternative_path}'). Trying HDFS fallback.")
            snapshot_path = hdfs_path
        
    try:
        print(f"Loading Customer Profile Snapshot from: {snapshot_path}")
        customer_snapshot_df = spark.read.parquet(snapshot_path)
        print(f"Loaded snapshot with {customer_snapshot_df.count()} customer profiles.")
    except Exception as e:
        print(f"Error loading customer snapshot: {e}")
        print("Starting stream without customer history (falling back to 0.0 margins).")
        # Tạo dataframe rỗng có schema để tránh crash
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType
        schema_empty = StructType([
            StructField("Customer_ID", StringType(), True),
            StructField("cust_hist_sales", DoubleType(), True),
            StructField("cust_hist_profit", DoubleType(), True),
            StructField("cust_hist_margin", DoubleType(), True)
        ])
        customer_snapshot_df = spark.createDataFrame([], schema_empty)

    # Tải PipelineModel tốt nhất
    try:
        model = load_pipeline_model(spark)
    except Exception as e:
        print(f"Critical error: Spark job cannot start without a trained model: {e}")
        return

    # Khởi tạo nguồn đọc (ReadStream) từ Kafka Input Topic
    print(f"Subscribing to Kafka topic: {INPUT_TOPIC} at {KAFKA_BOOTSTRAP_SERVER}...")
    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER) \
        .option("subscribe", INPUT_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse JSON từ message Kafka
    df_json = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), order_schema).alias("data")) \
        .select("data.*")

    # Lọc bỏ các message trống (Null)
    df_valid = df_json.filter(col("Order_ID").isNotNull() & col("Customer_ID").isNotNull())

    # JOIN với bảng Customer Snapshot bằng Customer_ID
    df_joined = df_valid.join(customer_snapshot_df, on="Customer_ID", how="left")

    # Điền giá trị mặc định cho các khách hàng mới chưa có lịch sử mua hàng
    df_joined = df_joined.na.fill({
        "cust_hist_sales": 0.0,
        "cust_hist_profit": 0.0,
        "cust_hist_margin": 0.0
    })

    # Chạy tiền xử lý tạo đặc trưng
    df_features = preprocess_realtime_features(df_joined)

    # Chạy mô hình dự báo và tính chỉ số rủi ro
    df_predictions = predict_realtime_profit(df_features, model)

    # Xác định đường dẫn lưu trữ checkpoint
    _, checkpoint_path = get_output_paths()
    print(f"Checkpoint location configured at: {checkpoint_path}")

    # Ghi dữ liệu streaming (WriteStream)
    query = df_predictions.writeStream \
        .foreachBatch(write_to_destinations) \
        .option("checkpointLocation", checkpoint_path) \
        .start()

    print("Spark Streaming Job is running. Waiting for incoming data...")
    query.awaitTermination()

if __name__ == "__main__":
    main()
