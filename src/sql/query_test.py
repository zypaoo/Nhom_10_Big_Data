import sys
sys.stdout.reconfigure(encoding='utf-8')

from src.ingestion.read_hdfs import spark

df = spark.table("superstore")
print("1. CẤU TRÚC SCHEMA DỮ LIỆU HDFS")
df.printSchema()
print("\n2. HIỂN THỊ MẪU 5 DÒNG DỮ LIỆU")
df.show(5, truncate=True)
print("\n3. TỔNG SỐ BẢN GHI ĐỌC ĐƯỢC TỪ HDFS")
total_records = df.count()
print(f"Tổng số bản ghi: {total_records} dòng")



