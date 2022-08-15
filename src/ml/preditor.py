import pandas as pd
from src.ml.utils import load_model

def _predict(data_df, model_path) -> pd.Series:
    '''hace la carga del modelo y realiza las predicciones correspondientes'''
    imported_model = load_model(model_path)
    prediction = pd.Series(imported_model.predict_proba(data_df)[:, -1], index=data_df.index)
    return prediction