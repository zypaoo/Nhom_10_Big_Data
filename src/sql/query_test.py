from src.ingestion.read_hdfs import read_superstore

df = read_superstore()

spark = df.sparkSession

print("Partitions:", df.rdd.getNumPartitions())
print("Default Parallelism:", spark.sparkContext.defaultParallelism)
print("Master:", spark.sparkContext.master)