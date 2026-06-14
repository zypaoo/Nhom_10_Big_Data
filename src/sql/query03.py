from src.streaming.ingestion.read_hdfs import spark
spark.sql("""
    WITH monthly_sales AS (
        SELECT
            YEAR(Order_Date) AS order_year,
            MONTH(Order_Date) AS order_month,
            ROUND(SUM(Sales), 2) AS total_sales,
            ROUND(SUM(Profit), 2) AS total_profit
        FROM superstore
        WHERE Order_Date IS NOT NULL
        GROUP BY YEAR(Order_Date), MONTH(Order_Date)
    ),
    rolling_and_mom AS (
        SELECT
            order_year,
            order_month,
            total_sales,
            LAG(total_sales) OVER (ORDER BY order_year, order_month) AS prev_month_sales,
            total_profit,
            LAG(total_profit) OVER (ORDER BY order_year, order_month) AS prev_month_profit
        FROM monthly_sales
    )
    SELECT
        order_year,
        order_month,
        total_sales,
        ROUND((total_sales - prev_month_sales) / prev_month_sales * 100, 2) AS sales_mom_growth_pct,
        total_profit,
        ROUND((total_profit - prev_month_profit) / prev_month_profit * 100, 2) AS profit_mom_growth_pct
    FROM rolling_and_mom
    ORDER BY order_year, order_month
""").show(20, truncate=False)



