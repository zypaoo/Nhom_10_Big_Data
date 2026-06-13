from src.ingestion.read_hdfs import spark
spark.sql("""
    WITH market_yearly AS (
        SELECT
            Market,
            YEAR(Order_Date) AS order_year,
            ROUND(SUM(Sales), 2) AS total_sales,
            ROUND(SUM(Profit), 2) AS total_profit
        FROM superstore
        WHERE Order_Date IS NOT NULL AND Market IS NOT NULL
        GROUP BY Market, YEAR(Order_Date)
    ),
    market_yoy AS (
        SELECT
            Market,
            order_year,
            total_sales,
            LAG(total_sales) OVER (PARTITION BY Market ORDER BY order_year) AS prev_year_sales,
            total_profit,
            LAG(total_profit) OVER (PARTITION BY Market ORDER BY order_year) AS prev_year_profit
        FROM market_yearly
    )
    SELECT
        Market,
        order_year,
        total_sales,
        ROUND((total_sales - prev_year_sales) / prev_year_sales * 100, 2) AS sales_growth_pct,
        total_profit,
        ROUND((total_profit - prev_year_profit) / prev_year_profit * 100, 2) AS profit_growth_pct
    FROM market_yoy
    ORDER BY Market, order_year
""").show(20, truncate=False)