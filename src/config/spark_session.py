import os
os.environ["HADOOP_USER_NAME"] = "vudua"

from pyspark.sql import SparkSession
from src.config.cluster_config import (
    MODE,
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
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(SPARK_MASTER)
    )
    
    # Chỉ giới hạn tài nguyên phân tán khi chạy trên Cluster thực tế để tránh tranh chấp cụm chung
    if MODE == "cluster":
        builder = (
            builder
            .config("spark.executor.memory", EXECUTOR_MEMORY)
            .config("spark.executor.cores", EXECUTOR_CORES)
            .config("spark.cores.max", MAX_CORES)
        )
    else:
        # Chế độ local: Sử dụng tối đa tài nguyên máy local (không giới hạn, tự động phát hiện bộ nhớ)
        try:
            import psutil
            total_mem_gb = int(psutil.virtual_memory().total / (1024 ** 3))
            # Chừa 3GB cho OS, tối thiểu là 4GB
            spark_mem_gb = max(total_mem_gb - 3, 4)
            local_mem = f"{spark_mem_gb}g"
        except Exception:
            local_mem = "12g"  # Mặc định hào phóng làm fallback
            
        builder = (
            builder
            .config("spark.driver.memory", local_mem)
            .config("spark.executor.memory", local_mem)
        )
        
    spark = (
        builder
        .config("spark.sql.shuffle.partitions", DEFAULT_SHUFFLE_PARTITIONS)
        .config("spark.default.parallelism", DEFAULT_PARALLELISM)
        .config("spark.driver.host", DRIVER_IP)
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.dynamicAllocation.enabled", "false")
        .config("spark.executorEnv.HADOOP_USER_NAME", "vudua")
        # Chỉ định thư mục tạm spark_temp trong dự án thay vì AppData Temp của Windows để tránh bị xóa file shuffle
        .config("spark.local.dir", "spark_temp")
        .getOrCreate()
    )
    try:
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        # Fix HDFS network timeout to failover quickly when worker2 is offline
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        hadoop_conf.set("dfs.client.socket-timeout", "3000")
        hadoop_conf.set("dfs.socket.timeout", "3000")
        hadoop_conf.set("ipc.client.connect.timeout", "3000")
        hadoop_conf.set("ipc.client.connect.max.retries.on.timeouts", "2")
    except Exception:
        pass
    return spark
