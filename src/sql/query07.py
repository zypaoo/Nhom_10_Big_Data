from src.streaming.ingestion.read_hdfs import spark

spark.sql("""
WITH product_profit AS (
   SELECT
       Category,
       Product_Name,
       COUNT(DISTINCT Order_ID) AS total_orders,
       ROUND(SUM(Sales), 2) AS total_sales,
       ROUND(SUM(Profit), 2) AS total_profit,
       ROUND(AVG(Profit), 2) AS avg_profit
   FROM superstore
   GROUP BY Category, Product_Name
),

ranked_products AS (
   SELECT
       *,
       DENSE_RANK() OVER (
           PARTITION BY Category
           ORDER BY total_profit DESC
       ) AS rank_profit_in_category
   FROM product_profit
)

SELECT
   Category,
   Product_Name,
   total_orders,
   total_sales,
   total_profit,
   avg_profit,
   rank_profit_in_category
FROM ranked_products
WHERE rank_profit_in_category <= 5
ORDER BY Category, rank_profit_in_category
""").show(20,truncate=False)