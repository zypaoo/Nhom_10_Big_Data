from src.ingestion.read_hdfs import read_superstore

df = read_superstore()

df.printSchema()