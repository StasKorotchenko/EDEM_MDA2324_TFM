import pickle
import pandas as pd
import numpy as np
import os
from datetime import datetime
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from google.cloud import bigquery, storage
import joblib
import yaml
import io

app = FastAPI()


bucket_name = "bucket_for_model_tfm"
kmeans_model_filename = "clusterizacion_clientes_model.pkl"
scaler_filename = "clusterizacion_clientes_modelscaler.pkl"
prophet_model_filename = "prophet_model.pkl"

project_id = "tfm-edem"
dataset_id = "tabla_pred_clust"
cluster_table_id = "pred_clust"
demand_table_id = "demand_predictions"


bq_client = bigquery.Client()
storage_client = storage.Client()


def load_model_from_gcp(model_filename, is_joblib=False):
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(model_filename)
    model_bytes = blob.download_as_bytes()

    if is_joblib:
        model = joblib.load(io.BytesIO(model_bytes))
    else:
        model = pickle.loads(model_bytes)
    
    print(f"Loaded model type: {type(model)}")
    return model


kmeans_model = load_model_from_gcp(kmeans_model_filename, is_joblib=False)
scaler = load_model_from_gcp(scaler_filename, is_joblib=False)
prophet_model = load_model_from_gcp(prophet_model_filename, is_joblib=True)


def load_schema_from_yaml(yaml_file):
    with open(yaml_file, 'r') as file:
        schema_dict = yaml.safe_load(file)
    schema = [bigquery.SchemaField(col['name'], col['type']) for col in schema_dict]
    return schema


def create_bq_table_if_not_exists(table_id, schema):
    dataset_ref = bq_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    try:
        bq_client.get_table(table_ref)
        print(f"Table {table_id} already exists.")
    except Exception:
        table = bigquery.Table(table_ref, schema=schema)
        bq_client.create_table(table)
        print(f"Table {table_id} created.")


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
    days: int
    start_date: str = Field(default=None, description="Дата начала предсказания в формате 'YYYY-MM-DD'")


def save_to_bigquery(data, prediction_result, table_id, schema_file):
    schema = load_schema_from_yaml(schema_file)

    formatted_data = {field.name: data.get(field.name, None) for field in schema}

    if 'prediction_result' in formatted_data:
        formatted_data['prediction_result'] = prediction_result

    formatted_data["timestamp"] = datetime.utcnow().isoformat()

    if 'ds' in formatted_data:
        try:
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
        
        input_df = pd.DataFrame([data.dict()])
        scaled_input_df = scaler.transform(input_df)
        prediction = kmeans_model.predict(scaled_input_df)[0]

        if isinstance(prediction, (np.integer, np.int32, np.int64)):
            prediction = int(prediction)
        elif isinstance(prediction, (np.floating, np.float32, np.float64)):
            prediction = float(prediction)

        
        save_to_bigquery(data.dict(), str(prediction), cluster_table_id, cluster_schema_file)

        return {"prediction": prediction}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/demand_predict")
def demand_predict(data: DemandInputData):
    try:
        n_days = data.days

        
        if data.start_date:
            current_date = datetime.strptime(data.start_date, "%Y-%m-%d")
        else:
            current_date = datetime.utcnow()

        
        future = prophet_model.make_future_dataframe(periods=n_days, freq='D', include_history=False)
        future['ds'] = pd.date_range(start=current_date, periods=n_days, freq='D')

        print(f"Future DataFrame before prediction: {future.head()}")
        print(f"Future DataFrame date range: {future['ds'].min()} - {future['ds'].max()}")

        
        forecast = prophet_model.predict(future)

        
        results = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail(n_days).to_dict(orient='records')

        
        for result in results:
            result['ds'] = result['ds'].strftime('%Y-%m-%d %H:%M:%S')

        
        for result in results:
            result_data = {
                "ds": result['ds'],
                "yhat": result['yhat'],
                "yhat_lower": result['yhat_lower'],
                "yhat_upper": result['yhat_upper'],
                "timestamp": datetime.utcnow().isoformat()
            }
            save_to_bigquery(result_data, None, demand_table_id, demand_schema_file)

        return {"forecast": results}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
