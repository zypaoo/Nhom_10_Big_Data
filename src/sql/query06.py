from src.streaming.ingestion.read_hdfs import spark
spark.sql("""
WITH customer_orders AS (
    SELECT
        Customer_ID,
        COUNT(DISTINCT Order_ID) AS total_orders,
        SUM(Sales) AS total_sales
    FROM superstore
    GROUP BY Customer_ID
),
customer_segments AS (
    SELECT
        Customer_ID,
        total_orders,
        total_sales,
        CASE
            WHEN total_orders = 1
                THEN 'One-time Customer'
            WHEN total_orders BETWEEN 2 AND 5
                THEN 'Regular Customer'
            ELSE 'Loyal Customer'
        END AS customer_group
    FROM customer_orders
)
SELECT
    customer_group,
    COUNT(*) AS total_customers,
    ROUND(AVG(total_orders), 2) AS avg_orders,
    ROUND(SUM(total_sales), 2) AS total_sales,
    ROUND(AVG(total_sales), 2) AS avg_customer_sales
FROM customer_segments
GROUP BY customer_group
ORDER BY total_sales DESC
""").show(20,truncate=False)
