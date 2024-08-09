import pandas as pd
from prophet import Prophet
from google.cloud import bigquery
from google.cloud import storage
import pickle
import json

# Cargar credenciales desde el archivo JSON
credentials_path = 'tfm-edem-4bb10bc4ec8e.json'
with open(credentials_path) as f:
    service_account_info = json.load(f)

# Configura el cliente de BigQuery
client = bigquery.Client.from_service_account_info(service_account_info)

# Consulta para extraer los datos de BigQuery
query = """
    SELECT *
    FROM tfm-edem.tablas_ml.ml_demanda
"""
df = client.query(query).to_dataframe()

# Supongamos que los datos tienen columnas 'ds' y 'y' necesarias para Prophet
model = Prophet(daily_seasonality=True)
model.fit(df)

# Guarda el modelo entrenado en un archivo pickle
model_filename = 'prophet_model.pkl'
with open(model_filename, 'wb') as f:
    pickle.dump(model, f)

# Configura el cliente de Google Cloud Storage
storage_client = storage.Client.from_service_account_info(service_account_info)
bucket_name = 'bucket_for_model_tfm'
bucket = storage_client.bucket(bucket_name)

# Sube el archivo pickle al bucket
blob = bucket.blob(model_filename)
blob.upload_from_filename(model_filename)

print(f"Modelo subido a gs://{bucket_name}/{model_filename}")