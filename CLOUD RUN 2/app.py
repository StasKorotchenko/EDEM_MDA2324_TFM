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

# Convertir la columna 'ds' al tipo datetime
df['ds'] = pd.to_datetime(df['ds'])

df = df.sort_values(by='ds')

df = df.iloc[:-7]

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

# Para las predicciones, renombrar la columna 'yhat' a 'y' ya que corresponde a la demanda predicha
forecast_filtered = forecast_filtered.rename(columns={'yhat': 'y'})

# Combinar los datos históricos (con columna 'y') y las predicciones (donde 'y' corresponde a 'yhat')
df_combined = pd.concat([df[['ds', 'y']], forecast_filtered[['ds', 'y', 'yhat_lower', 'yhat_upper']]], ignore_index=True)

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

# Configurar la tabla de destino en BigQuery para las predicciones
table_id = 'tfm-edem.tablas_ml.ml_demanda_pred'

# Definir el esquema de la tabla para las predicciones
job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("ds", "TIMESTAMP"),
        bigquery.SchemaField("y", "FLOAT"),  # Ahora y es la demanda predicha
        bigquery.SchemaField("yhat_lower", "FLOAT"),
        bigquery.SchemaField("yhat_upper", "FLOAT"),
    ],
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE  # Sobrescribir la tabla si ya existe
)

# Subir los datos de predicción a la tabla en BigQuery
job = client.load_table_from_dataframe(forecast_filtered, table_id, job_config=job_config)
job.result()  # Esperar a que el trabajo termine

print(f"Predicciones guardadas en la tabla {table_id}")

# Configurar la tabla de destino para los datos reales + predicción
combined_table_id = 'tfm-edem.tablas_ml.ml_demanda_tabla'

# Definir el esquema de la tabla para los datos reales + predicción
combined_job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("ds", "TIMESTAMP"),
        bigquery.SchemaField("y", "FLOAT"),  # La columna y contendrá valores reales y predicciones (yhat)
        bigquery.SchemaField("yhat_lower", "FLOAT"),
        bigquery.SchemaField("yhat_upper", "FLOAT"),
    ],
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE  # Sobrescribir la tabla si ya existe
)

# Subir los datos reales + predicción a la tabla en BigQuery
combined_job = client.load_table_from_dataframe(df_combined, combined_table_id, job_config=combined_job_config)
combined_job.result()  # Esperar a que el trabajo termine

print(f"Datos reales + predicciones guardadas en la tabla {combined_table_id}")
