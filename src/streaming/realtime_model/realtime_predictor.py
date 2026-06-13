from pyspark.sql.functions import col, exp, when, lit, current_timestamp

def predict_realtime_profit(df, pipeline_model):
    """
    Thực hiện dự đoán lợi nhuận realtime trên Spark Structured Streaming:
    1. model.transform(): Chạy dự báo qua toàn bộ ML Pipeline.
    2. Quy đổi ngược hàm log (Inverse Log Transform): prediction_usd = exp(prediction) - 3000.
    3. Phân loại rủi ro lợi nhuận (Rule Engine).
    4. Gán nhãn thời gian dự đoán.
    """
    # 1. Thực hiện dự đoán từ PipelineModel (StringIndexer -> VectorAssembler -> RandomForest)
    predictions = pipeline_model.transform(df)
    
    # 2. Chuyển đổi ngược thang log về đơn vị tiền tệ USD gốc (c_shift = 3000.0)
    predictions_usd = predictions.withColumn("prediction_usd", exp(col("prediction")) - 3000.0)
    
    # 3. Phân loại mức độ lợi nhuận/rủi ro bằng Rule Engine
    predictions_risk = predictions_usd.withColumn("risk_level",
        when(col("prediction_usd") < 0.0, lit("LOSS RISK"))
        .when((col("prediction_usd") >= 0.0) & (col("prediction_usd") <= 100.0), lit("LOW PROFIT"))
        .when((col("prediction_usd") > 100.0) & (col("prediction_usd") <= 500.0), lit("MEDIUM PROFIT"))
        .otherwise(lit("HIGH PROFIT OPPORTUNITY"))
    )
    
    # 4. Bổ sung thời gian dự đoán cho thông tin kiểm toán (audit log)
    final_df = predictions_risk.withColumn("prediction_time", current_timestamp().cast("string"))
    
    return final_df
