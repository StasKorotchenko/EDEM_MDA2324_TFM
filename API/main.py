
# main.py
from fastapi import FastAPI, HTTPException
from google.cloud import storage
import pickle
import pandas as pd
from pydantic import BaseModel
import os

# Инициализация FastAPI
app = FastAPI()

# Конфигурация Google Cloud Storage
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/korotchenkostanislav/Documents/EDEM/TFM/tfm-edem-54bbbe821339.json"
bucket_name = "bucket_for_model_tfm"
model_filename = "clusterizacion_clientes_model.pkl"

# Загрузка модели из GCP
def load_model_from_gcp():
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(model_filename)
    model_bytes = blob.download_as_bytes()
    model = pickle.loads(model_bytes)
    print(f"Loaded model type: {type(model)}")  # Отладочный вывод
    return model

model = load_model_from_gcp()

class InputData(BaseModel):
    total_spent: float
    purchase_frequency: float
    average_order_value: float
    num_reviews: float
    avg_review_score: float

# Эндпоинт для предсказаний
@app.post("/predict")
def predict(data: InputData):
    try:
        # Преобразование входных данных в DataFrame
        input_df = pd.DataFrame([data.dict()])
        
        # Получение предсказания
        prediction = model.predict(input_df)
        
        return {"prediction": prediction.tolist()}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# Запуск сервера: uvicorn main:app --reload
