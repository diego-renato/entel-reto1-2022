import joblib
import feature_engine
import lightgbm
import category_encoders
from src.ml.transformer import DataframeColumnDuplicateTransformer

def load_model(model_path: str) :
    return joblib.load(model_path)

