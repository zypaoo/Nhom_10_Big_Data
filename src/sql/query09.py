from src.streaming.ingestion.read_hdfs import spark

spark.sql("""
WITH order_level AS (
   SELECT
       Order_ID,
       MIN(Order_Date) AS order_date,
       FIRST(Customer_Name) AS customer_name,
       FIRST(Market) AS market,
       FIRST(Segment) AS segment,
       FIRST(Ship_Mode) AS ship_mode,
       ROUND(SUM(Sales), 2) AS order_sales,
       ROUND(SUM(Profit), 2) AS order_profit,
       ROUND(SUM(Shipping_Cost), 2) AS order_shipping_cost,
       SUM(Quantity) AS total_items
   FROM superstore
   GROUP BY Order_ID
),

ranked_orders AS (
   SELECT
       *,
       NTILE(10) OVER (ORDER BY order_sales DESC) AS sales_decile
   FROM order_level
)

SELECT
   Order_ID,
   customer_name,
   market,
   segment,
   ship_mode,
   order_sales,
   order_profit,
   order_shipping_cost,
   total_items
FROM ranked_orders
WHERE sales_decile = 1
   AND order_profit < 0
ORDER BY order_sales DESC
LIMIT 10
""").show(20,truncate=False)