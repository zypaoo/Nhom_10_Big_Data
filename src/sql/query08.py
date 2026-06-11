from src.ingestion.read_hdfs import spark

spark.sql("""
WITH discount_groups AS (
   SELECT
       Order_ID,
       Sales,
       Profit,
       Discount,
       CASE
           WHEN Discount = 0 THEN 'No Discount'
           WHEN Discount <= 0.10 THEN 'Low Discount (0-10%)'
           WHEN Discount <= 0.30 THEN 'Medium Discount (10-30%)'
           ELSE 'High Discount (>30%)'
       END AS discount_group
   FROM superstore
)

SELECT
   discount_group,
   COUNT(DISTINCT Order_ID) AS total_orders,
   ROUND(AVG(Discount), 2) AS avg_discount,
   ROUND(SUM(Sales), 2) AS total_sales,
   ROUND(SUM(Profit), 2) AS total_profit,
   ROUND(SUM(Profit) / SUM(Sales) * 100, 2) AS profit_margin_pct,
   ROUND(
       SUM(CASE WHEN Profit < 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
       2
   ) AS negative_profit_pct
FROM discount_groups
GROUP BY discount_group
ORDER BY
   CASE discount_group
       WHEN 'No Discount' THEN 1
       WHEN 'Low Discount (0-10%)' THEN 2
       WHEN 'Medium Discount (10-30%)' THEN 3
       ELSE 4
   END
""").show(20,truncate=False)