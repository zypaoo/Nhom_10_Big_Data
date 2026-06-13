import sys
import os
from pyspark.ml import PipelineModel
from src.config.cluster_config import MODE

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

_cached_model = None

def load_pipeline_model(spark, model_path=None):
    """
    Tải toàn bộ PipelineModel (StringIndexer, OneHotEncoder, VectorAssembler, RandomForestRegressor)
    Mô hình được cache trong bộ nhớ JVM của Spark để tái sử dụng cho realtime inference.
    """
    global _cached_model
    if _cached_model is not None:
        return _cached_model
        
    if model_path is None:
        hdfs_path = "hdfs://master:9000/bigdata/ml2/models/best_rf_pipeline_model"
        if MODE == "cluster":
            model_path = hdfs_path
        else:
            # Đường dẫn tìm kiếm mặc định trong thư mục streaming/models/
            local_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "models", "best_rf_pipeline_model"))
            alternative_path = os.path.abspath(os.path.join(os.getcwd(), "src", "streaming", "models", "best_rf_pipeline_model"))
            
            if os.path.exists(local_path):
                model_path = local_path
            elif os.path.exists(alternative_path):
                model_path = alternative_path
            else:
                model_path = hdfs_path
            
    print(f"Loading PipelineModel from: {model_path} ...")
    try:
        _cached_model = PipelineModel.load(model_path)
        print("PipelineModel loaded successfully!")
        return _cached_model
    except Exception as e:
        print(f"Error loading PipelineModel: {e}")
        # Thử đường dẫn thay thế dự phòng
        fallback_path = "models/best_rf_pipeline_model"
        print(f"Trying fallback path: {fallback_path} ...")
        try:
            _cached_model = PipelineModel.load(fallback_path)
            print("PipelineModel loaded successfully from fallback path!")
            return _cached_model
        except Exception as fallback_err:
            print(f"Critical error: Could not load model from any path: {fallback_err}")
            raise fallback_err
