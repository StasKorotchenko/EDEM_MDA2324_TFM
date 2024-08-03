import pickle
import pandas as pd
import numpy as np
import os
from datetime import datetime
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from google.cloud import bigquery, storage
import joblib
import yaml
import io


app = FastAPI()

# Конфигурация Google Cloud Storage и BigQuery
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/korotchenkostanislav/Documents/EDEM/TFM/tfm-edem-54bbbe821339.json"
bucket_name = "bucket_for_model_tfm"
kmeans_model_filename = "clusterizacion_clientes_model.pkl"
prophet_model_filename = "prophet_model.pkl"

project_id = "tfm-edem"
dataset_id = "tabla_pred_clust"
cluster_table_id = "pred_clust"
demand_table_id = "demand_predictions"

# Инициализация клиентов
bq_client = bigquery.Client()
storage_client = storage.Client()

def load_model_from_gcp(model_filename, is_joblib=False):
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(model_filename)
    model_bytes = blob.download_as_bytes()

    # Загружаем модель из байтового потока
    if is_joblib:
        model = joblib.load(io.BytesIO(model_bytes))
    else:
        model = pickle.loads(model_bytes)
    
    print(f"Loaded model type: {type(model)}")  # Отладочный вывод
    return model

# Загрузка моделей из GCP
kmeans_model = load_model_from_gcp(kmeans_model_filename, is_joblib=False)
prophet_model = load_model_from_gcp(prophet_model_filename, is_joblib=True)

# Загрузка схемы из YAML-файла
def load_schema_from_yaml(yaml_file):
    with open(yaml_file, 'r') as file:
        schema_dict = yaml.safe_load(file)
    schema = [bigquery.SchemaField(col['name'], col['type']) for col in schema_dict]
    return schema

# Создание таблицы в BigQuery, если она не существует
def create_bq_table_if_not_exists(table_id, schema):
    dataset_ref = bq_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    try:
        bq_client.get_table(table_ref)  # Проверка, существует ли таблица
        print(f"Table {table_id} already exists.")
    except Exception:
        # Если таблица не существует, создаем ее
        table = bigquery.Table(table_ref, schema=schema)
        bq_client.create_table(table)
        print(f"Table {table_id} created.")

# Загрузка схем и создание таблиц
cluster_schema_file = "cluster_schema.yaml"
demand_schema_file = "demand_schema.yaml"
cluster_schema = load_schema_from_yaml(cluster_schema_file)
demand_schema = load_schema_from_yaml(demand_schema_file)
create_bq_table_if_not_exists(cluster_table_id, cluster_schema)
create_bq_table_if_not_exists(demand_table_id, demand_schema)

class ClusterInputData(BaseModel):
    total_spent: float
    purchase_frequency: float
    average_order_value: float
    num_reviews: int
    avg_review_score: float
    days_since_last_purchase: float

class DemandInputData(BaseModel):
    days: int  # Количество дней для прогноза

def save_to_bigquery(data, prediction_result, table_id, schema_file):
    schema = load_schema_from_yaml(schema_file)
    
    # Преобразование данных в формат, соответствующий схеме BigQuery
    formatted_data = {field.name: data.get(field.name, None) for field in schema}

    # Вставка данных зависит от типа модели
    if 'prediction_result' in formatted_data:  # Если поле prediction_result существует в схеме
        formatted_data['prediction_result'] = prediction_result

    # Добавляем метку времени
    formatted_data["timestamp"] = datetime.utcnow().isoformat()

    # Проверка и форматирование даты
    if 'ds' in formatted_data:
        try:
            # Убедитесь, что форматирование даты соответствует типу в BigQuery
            formatted_data['ds'] = datetime.strptime(formatted_data['ds'], '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
        except ValueError:
            print(f"Error formatting date: {formatted_data['ds']}")
            formatted_data['ds'] = None
    
    rows_to_insert = [formatted_data]
    errors = bq_client.insert_rows_json(f"{project_id}.{dataset_id}.{table_id}", rows_to_insert)
    if errors:
        print(f"Failed to insert rows into BigQuery: {errors}")

@app.post("/predict")
def predict(data: ClusterInputData):
    try:
        # Преобразование входных данных в DataFrame
        input_df = pd.DataFrame([data.dict()])
        
        # Получение предсказания
        prediction = kmeans_model.predict(input_df)[0]
        
        # Преобразуем prediction в стандартный Python тип (int)
        if isinstance(prediction, (np.integer, np.int32, np.int64)):
            prediction = int(prediction)
        elif isinstance(prediction, (np.floating, np.float32, np.float64)):
            prediction = float(prediction)
        
        # Сохранение данных и предсказания в BigQuery
        save_to_bigquery(data.dict(), str(prediction), cluster_table_id, cluster_schema_file)
        
        return {"prediction": prediction}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/demand_predict")
def demand_predict(data: DemandInputData):
    try:
        # Получение количества дней для предсказания
        n_days = data.days
        
        # Создание future DataFrame на n дней в будущем
        # Проверьте текущую дату
        current_date = datetime.utcnow()
        
        # Создание future DataFrame с учетом текущей даты
        future = prophet_model.make_future_dataframe(periods=n_days, freq='D', include_history=False)
        future['ds'] = pd.date_range(start=current_date, periods=n_days, freq='D')
        
        # Логирование для проверки
        print(f"Future DataFrame before prediction: {future.head()}")
        print(f"Future DataFrame date range: {future['ds'].min()} - {future['ds'].max()}")
        
        # Получение прогноза
        forecast = prophet_model.predict(future)
        
        # Форматирование данных для ответа
        results = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail(n_days).to_dict(orient='records')
        
        # Преобразование даты в строку только для результата
        for result in results:
            result['ds'] = result['ds'].strftime('%Y-%m-%d %H:%M:%S')
            
        # Сохранение прогноза в BigQuery
        for result in results:
            result_data = {
                "ds": result['ds'],  # Дата уже отформатирована
                "yhat": result['yhat'],
                "yhat_lower": result['yhat_lower'],
                "yhat_upper": result['yhat_upper'],
                "timestamp": datetime.utcnow().isoformat()
            }
            save_to_bigquery(result_data, None, demand_table_id, demand_schema_file)
        
        return {"forecast": results}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

