from src.streaming.ingestion.read_hdfs import spark

spark.sql("""
WITH segment_category AS (
   SELECT
       Segment,
       Category,
       COUNT(DISTINCT Order_ID) AS total_orders,
       ROUND(SUM(Sales), 2) AS total_sales,
       ROUND(SUM(Profit), 2) AS total_profit,
       ROUND(
           SUM(Profit) * 100.0 / NULLIF(SUM(Sales), 0),
           2
       ) AS profit_margin_pct
   FROM superstore
   WHERE Segment IN ('Consumer', 'Corporate', 'Home Office')
     AND Category IS NOT NULL
   GROUP BY Segment, Category
),

ranked_result AS (
   SELECT
       *,
       DENSE_RANK() OVER (
           PARTITION BY Segment
           ORDER BY total_profit DESC
       ) AS profit_rank
   FROM segment_category
)

SELECT
   Segment,
   Category,
   total_orders,
   total_sales,
   total_profit,
   profit_margin_pct,
   profit_rank
FROM ranked_result
ORDER BY Segment, profit_rank
""").show(20,truncate=False)