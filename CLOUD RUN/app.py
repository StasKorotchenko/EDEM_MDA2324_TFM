import os
import pandas as pd
from google.cloud import bigquery, storage
from sklearn.cluster import KMeans
import joblib

# Configuración de Google Cloud
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "tfm-edem-4bb10bc4ec8e.json"

# Cliente de BigQuery
client = bigquery.Client()

# Leer la tabla de BigQuery
query = "SELECT * FROM `tfm-edem.tablas_ml.ml_clusterizacion`"
df = client.query(query).to_dataframe()

# Separar la columna customer_id
customer_ids = df['customer_id']
df = df.drop(columns=['customer_id'])

# Entrenar el modelo de clusterización
kmeans = KMeans(n_clusters=9, random_state=42)
clusters = kmeans.fit_predict(df)

# Crear un nuevo dataframe con customer_id y los clusters
result_df = pd.DataFrame({
    'customer_id': customer_ids,
    'cluster': clusters
})

# Definir el ID de la tabla temporal y de la tabla principal
temp_table_id = "tfm-edem.tablas_ml.ml_clusterizacion_temp"
main_table_id = "tfm-edem.tablas_ml.ml_clusterizacion_bi"

# Crear la tabla temporal en BigQuery
schema = [
    bigquery.SchemaField("customer_id", "STRING"),
    bigquery.SchemaField("cluster", "INTEGER"),
]

table = bigquery.Table(temp_table_id, schema=schema)
client.create_table(table, exists_ok=True)  # Crear la tabla temporal

# Cargar el DataFrame en la tabla temporal
job = client.load_table_from_dataframe(result_df, temp_table_id)
job.result()  # Esperar a que el trabajo termine

# Actualizar la tabla principal con los datos de la tabla temporal
update_query = f"""
MERGE `{main_table_id}` AS target
USING `{temp_table_id}` AS source
ON target.customer_id = source.customer_id
WHEN MATCHED THEN
  UPDATE SET target.cluster = source.cluster
"""

client.query(update_query).result()  # Ejecutar la consulta de actualización

# Eliminar la tabla temporal
client.delete_table(temp_table_id)

# Guardar el modelo en un archivo pkl
model_filename = 'kmeans_model.pkl'
joblib.dump(kmeans, model_filename)

# Subir el modelo al bucket de Google Cloud Storage
bucket_name = 'bucket_for_model_tfm'
storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(model_filename)
blob.upload_from_filename(model_filename)

print("Script completado con éxito.")
