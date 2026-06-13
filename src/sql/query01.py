from src.ingestion.read_hdfs import spark
spark.sql("""
    SELECT
        Market,
        COUNT(Order_ID) AS total_orders,
        ROUND(SUM(Sales), 2) AS total_sales,
        ROUND(SUM(Profit), 2) AS total_profit,
        ROUND(SUM(Profit)/SUM(Sales)*100, 2) AS profit_margin_pct
    FROM superstore
    GROUP BY Market
    ORDER BY total_sales DESC
""").show(20,truncate=False)