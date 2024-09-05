import pandas as pd
from prophet import Prophet
from google.cloud import bigquery
from google.cloud import storage
import pickle
import json

# Cargar credenciales desde el archivo JSON
credentials_path = 'tfm-edem-ec8bf5197ad5.json'
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

# Obtener el último valor de 'ds' en el dataframe para empezar las predicciones
last_date = df['ds'].max()

# Crear un dataframe con 183 días en el futuro, empezando desde el día siguiente al último valor de 'ds'
future = model.make_future_dataframe(periods=183, include_history=False)
future['ds'] = pd.date_range(start=last_date + pd.Timedelta(days=1), periods=183)

# Realizar la predicción
forecast = model.predict(future)

# Seleccionar las columnas de interés de la predicción
forecast_filtered = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]

# Guardar el modelo entrenado en un archivo pickle
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

# Configurar la tabla de destino en BigQuery
table_id = 'tfm-edem.tablas_ml.ml_demanda_pred'

# Definir el esquema de la tabla
job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("ds", "TIMESTAMP"),
        bigquery.SchemaField("yhat", "FLOAT"),
        bigquery.SchemaField("yhat_lower", "FLOAT"),
        bigquery.SchemaField("yhat_upper", "FLOAT"),
    ],
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE  # Sobrescribir la tabla si ya existe
)

# Subir los datos de predicción a la tabla en BigQuery
job = client.load_table_from_dataframe(forecast_filtered, table_id, job_config=job_config)
job.result()  # Esperar a que el trabajo termine

print(f"Predicciones guardadas en la tabla {table_id}")
