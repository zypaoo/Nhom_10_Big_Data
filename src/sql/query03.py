from src.ingestion.read_hdfs import spark
spark.sql("""
      SELECT
        YEAR(Order_Date) AS year,
        MONTH(Order_Date) AS month,
        COUNT(Order_ID) AS total_orders,
        ROUND(SUM(Sales), 2) AS total_sales,
        ROUND(SUM(Profit), 2) AS total_profit
    FROM superstore
    GROUP BY YEAR(Order_Date), MONTH(Order_Date)
    ORDER BY year ASC, month ASC
""").show(20,truncate=False)



