import pandas as pd
from google.cloud import storage, bigquery
import io
import functions_framework
from google.api_core.exceptions import NotFound

def process_and_load_data(event, context):
    bucket_name = 'cargacsv2ml'
    file_names = ['orders.csv', 'order_items.csv', 'order_payments.csv', 'reviews.csv', 'customers.csv']
    project_id = 'tfm-edem'
    dataset_id = 'tablas_ml'
    table_id = 'customer_features'

    storage_client = storage.Client()
    bq_client = bigquery.Client()

    # Inicializa un DataFrame vacío para combinar los datos
    combined_df = pd.DataFrame()

    bucket = storage_client.bucket(bucket_name)
    for file_name in file_names:
        blob = bucket.blob(file_name)
        if not blob.exists():
            print(f"File {file_name} does not exist in bucket {bucket_name}.")
            continue

        print(f"Downloading {file_name} from bucket {bucket_name}.")
        data = blob.download_as_text(encoding='utf-8')
        df = pd.read_csv(io.StringIO(data))

        # Imprime las primeras filas para depuración
        print(f"Data from {file_name}:")
        print(df.head())

        # Concatena con el DataFrame combinado
        combined_df = pd.concat([combined_df, df], ignore_index=True, sort=False)

    # Imprime las columnas del DataFrame combinado para depuración
    print("Columns in combined_df:")
    print(combined_df.columns)
    print("First few rows of combined_df:")
    print(combined_df.head())

    # Agregar columna ficticia 'days_since_purchase' si no existe
    if 'days_since_purchase' not in combined_df.columns:
        combined_df['days_since_purchase'] = None

    # Agregaciones específicas
    aggregations = {
        'amount': 'sum',
        'order_id': 'count',
        'price': 'mean',
        'review_id': 'count',
        'score': 'mean',
        'days_since_purchase': 'min'
    }

    customer_features = combined_df.groupby('customer_id').agg(aggregations).rename(columns={
        'amount': 'total_spent',
        'order_id': 'purchase_frequency',
        'price': 'average_order_value',
        'review_id': 'num_reviews',
        'score': 'avg_review_score'
    }).reset_index()

    # Definir el esquema
    schema = [
        bigquery.SchemaField("customer_id", "STRING"),
        bigquery.SchemaField("total_spent", "FLOAT64"),
        bigquery.SchemaField("purchase_frequency", "FLOAT64"),
        bigquery.SchemaField("average_order_value", "FLOAT64"),
        bigquery.SchemaField("num_reviews", "FLOAT64"),
        bigquery.SchemaField("avg_review_score", "FLOAT64"),
        bigquery.SchemaField("days_since_purchase", "FLOAT64")
    ]

    dataset_ref = bq_client.dataset(dataset_id)

    # Crear el dataset si no existe
    try:
        bq_client.get_dataset(dataset_ref)
    except NotFound:
        print(f"Dataset {dataset_id} does not exist. Creating it.")
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"  # Ajusta la ubicación según sea necesario
        bq_client.create_dataset(dataset)
        print(f"Dataset {dataset_id} created.")

    table_ref = dataset_ref.table(table_id)

    # Crear la tabla si no existe
    try:
        bq_client.get_table(table_ref)
        print(f"Table {table_id} already exists.")
    except NotFound:
        print(f"Table {table_id} does not exist. Creating it.")
        table = bigquery.Table(table_ref, schema=schema)
        table = bq_client.create_table(table)
        print(f"Table {table_id} created.")

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_TRUNCATE",
        source_format=bigquery.SourceFormat.CSV,
    )

    csv_buffer = io.StringIO()
    customer_features.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    print(f"Loading data into BigQuery table {table_id}.")
    job = bq_client.load_table_from_file(csv_buffer, table_ref, job_config=job_config)
    job.result()

    if job.error_result:
        print(f"Error: {job.error_result}")
    else:
        print(f'Loaded data into {project_id}.{dataset_id}.{table_id}')

@functions_framework.cloud_event
def hello_gcs(cloud_event):
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

    process_and_load_data(cloud_event, None)