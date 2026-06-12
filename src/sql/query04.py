from src.ingestion.read_hdfs import spark

spark.sql("""
WITH customer_sales AS (
    SELECT
        Customer_ID,
        Customer_Name,
        SUM(Sales) AS total_sales
    FROM superstore
    GROUP BY
        Customer_ID,
        Customer_Name
),
grand_total AS (
    SELECT SUM(total_sales) AS grand_total
    FROM customer_sales
),

ranked_customers AS (
    SELECT
        Customer_ID,
        Customer_Name,
        total_sales,
        SUM(total_sales) OVER (
            ORDER BY total_sales DESC
            ROWS BETWEEN UNBOUNDED PRECEDING
            AND CURRENT ROW
        ) AS running_sales,
        ROW_NUMBER() OVER (
            ORDER BY total_sales DESC
        ) AS customer_rank
    FROM customer_sales
)
SELECT
    r.Customer_ID,
    r.Customer_Name,
    ROUND(r.total_sales, 2) AS total_sales,
    ROUND(r.running_sales, 2) AS running_sales,
    ROUND(r.running_sales * 100.0 / g.grand_total,2) AS cumulative_sales_pct,
    r.customer_rank
FROM ranked_customers r
CROSS JOIN grand_total g
ORDER BY total_sales DESC
""").show(20,truncate=False)