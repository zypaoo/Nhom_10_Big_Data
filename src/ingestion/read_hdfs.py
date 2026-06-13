from src.config.spark_session import get_spark
from src.config.hdfs_config import SUPERSTORE_DATASET
from src.config.schema import SUPERSTORE_SCHEMA

spark = get_spark()
(
    spark.read
    .option("header", "true")
    .option("dateFormat", "yyyy-MM-dd")
    .schema(SUPERSTORE_SCHEMA)
    .csv(SUPERSTORE_DATASET)
    .createOrReplaceTempView("superstore")
)


