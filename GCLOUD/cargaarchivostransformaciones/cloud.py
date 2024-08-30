import functions_framework
import logging
import os
import traceback
import re
import io
import pandas as pd
import yaml
from google.cloud import bigquery
from google.cloud import storage
from google.api_core.exceptions import NotFound

# Leer la configuración del archivo YAML
with open("./schemas.yaml") as schema_file:
    config = yaml.load(schema_file, Loader=yaml.Loader)

PROJECT_ID = os.getenv('GCP_PROJECT')
BQ_DATASET = 'tablas'
CS = storage.Client()
BQ = bigquery.Client()

PROCESSED_PREFIX = "processed_"

def clean_csv(bucket_name, file_name, int_columns, float_columns, date_columns, processed_prefix):
    """Descarga el CSV, limpia columnas de enteros y decimales, transforma fechas y horas, y luego vuelve a subir el archivo limpio."""

    bucket = CS.bucket(bucket_name)
    cleaned_file_name = processed_prefix + file_name
    cleaned_blob = bucket.blob(cleaned_file_name)
    if cleaned_blob.exists():
        print(f"El archivo {cleaned_file_name} ya ha sido procesado. Omitting.")
        return cleaned_file_name

    # Descargar el archivo CSV y cargarlo en un DataFrame
    blob = bucket.blob(file_name)
    csv_content = blob.download_as_text()
    df = pd.read_csv(io.StringIO(csv_content))

    # Procesar columna 'amount' si existe
    if 'amount' in df.columns:
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
        mean_amount = df[df['amount'] <= 10000.0]['amount'].mean()
        df['amount'] = df['amount'].apply(lambda x: mean_amount if pd.notna(x) and x > 10000.0 else x)

    # Procesar columnas de enteros
    for col in int_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            mode_value = df[col].mode()
            if not mode_value.empty:
                mode_value = mode_value[0]
            else:
                mode_value = df[col].mean()
            df[col] = df[col].fillna(mode_value).astype('Int64') 

    # Procesar columnas de flotantes
    for col in float_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            mean_value = df[col].mean()
            df[col] = df[col].fillna(mean_value)


    dates = ["purchase_timestamp", "approved_at", "delivered_courier_date", "delivered_customer_date", "estimated_delivery_date"]
    # Asegurarse de que no haya duplicados combinando las listas
    all_date_columns = list(set(dates + date_columns))
    # Convertir todas las columnas de fecha a tipo datetime y cargar nulos como NaT a BigQuery
    for date_col in all_date_columns:
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            # Reemplazar fechas anteriores a 2020 por NaT
            df.loc[df[date_col] < pd.Timestamp('2020-01-01'), date_col] = pd.NaT

    # Convertir el DataFrame de nuevo a CSV y subirlo al bucket
    cleaned_csv_content = df.to_csv(index=False)
    cleaned_blob.upload_from_string(cleaned_csv_content, content_type="text/csv")
    return cleaned_file_name



def streaming(data):
    """Procesa el evento de carga de archivo y carga el CSV limpio a BigQuery."""
    bucketname = data['bucket']
    filename = data['name']

    # Verificar si el archivo ya ha sido procesado
    if filename.startswith(PROCESSED_PREFIX):
        print(f"File {filename} has already been processed. Skipping.")
        return

    try:
        for table in config:
            tableName = table.get('name')
            if re.search(tableName.replace('_', '-'), filename) or re.search(tableName, filename):
                tableSchema = table.get('schema')
                tableFormat = table.get('format')

                # Identificar las columnas de tipo entero, flotante y de fecha
                int_columns = [col['name'] for col in tableSchema if col['type'] == 'INTEGER']
                float_columns = [col['name'] for col in tableSchema if col['type'] == 'FLOAT']
                date_columns = [col['name'] for col in tableSchema if col['type'] == 'DATE']

                # Verificar si la tabla existe y crearla si es necesario
                _check_if_table_exists(tableName, tableSchema)

                if tableFormat == 'CSV':
                    # Limpiar el archivo CSV de filas nulas y con valores no válidos en columnas de enteros y decimales
                    cleaned_filename = clean_csv(bucketname, filename, int_columns, float_columns, date_columns, PROCESSED_PREFIX)

                    # Cargar el archivo limpio en BigQuery
                    _load_table_from_uri(bucketname, cleaned_filename, tableSchema, tableName)
    except Exception:
        print('Error streaming file. Cause: %s' % (traceback.format_exc()))

def _check_if_table_exists(tableName, tableSchema):
    """Verifica si la tabla existe en BigQuery y la crea si no existe."""
    table_id = BQ.dataset(BQ_DATASET).table(tableName)
    try:
        BQ.get_table(table_id)
        return True
    except NotFound:
        logging.warning(f'Creating table: {tableName}')
        schema = create_schema_from_yaml(tableSchema)
        table = bigquery.Table(table_id, schema=schema)
        table = BQ.create_table(table)
        print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")
        return False

def _load_table_from_uri(bucket_name, file_name, tableSchema, tableName):
    """Carga el archivo CSV desde el bucket a la tabla de BigQuery."""
    # Construye la URI del archivo en el bucket
    uri = f'gs://{bucket_name}/{file_name}'

    # Define el ID de la tabla en BigQuery
    table_id = f'{BQ_DATASET}.{tableName}'

    # Crea el esquema desde el archivo YAML
    schema = create_schema_from_yaml(tableSchema)
    print(schema)

    # Configura el job de carga
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Si el archivo CSV tiene una fila de encabezado
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        max_bad_records=10  # Permite hasta 10 errores antes de fallar
    )

    # Inicia el job de carga
    load_job = BQ.load_table_from_uri(
        uri,
        table_id,
        job_config=job_config,
    )

    # Espera a que el job de carga termine
    try:
        load_job.result()
        print(f"Loaded {load_job.output_rows} rows into {table_id}.")
    except Exception as e:
        print(f"Failed to load table: {e}")

def create_schema_from_yaml(table_schema):
    """Crea el esquema de BigQuery a partir del archivo YAML."""
    schema = []
    for column in table_schema:
        schemaField = bigquery.SchemaField(column['name'], column['type'], column['mode'])
        schema.append(schemaField)
        if column['type'] == 'RECORD' and 'fields' in column:
            schemaField.fields = create_schema_from_yaml(column['fields'])
    return schema

@functions_framework.cloud_event
def hello_gcs(cloud_event):
    """Función principal que maneja el evento de carga de archivo en GCS."""
    data = cloud_event.data
    event_id = cloud_event.get("id")
    event_type = cloud_event.get("type")
    bucket = data.get("bucket")
    name = data.get("name")
    metageneration = data.get("metageneration")
    timeCreated = data.get("timeCreated")
    updated = data.get("updated")
    print(f"Event ID: {event_id}")
    print(f"Event type: {event_type}")
    print(f"Bucket: {bucket}")
    print(f"File: {name}")
    print(f"Metageneration: {metageneration}")
    print(f"Created: {timeCreated}")
    print(f"Updated: {updated}")
    streaming(data)
