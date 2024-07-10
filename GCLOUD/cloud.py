import csv
from google.cloud import storage, bigquery

def process_csv(event, context):
    
    bucket_name = event['bucket']
    file_name = event['name']

    
    storage_client = storage.Client()
    bq_client = bigquery.Client()

    
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    csv_data = blob.download_as_text().splitlines()

    
    csv_reader = csv.reader(csv_data)

    
    headers = next(csv_reader)


    table_id = "tfm-edem.customers.Customers"

    
    rows_to_insert = []
    for row in csv_reader:
        row_dict = {
            headers[0]: int(row[0]),  
            headers[1]: row[1],
            headers[2]: row[2],
        }
        rows_to_insert.append(row_dict)

    
    errors = bq_client.insert_rows_json(table_id, rows_to_insert)
    if errors:
        print(f"Errors occurred while inserting rows: {errors}")
    else:
        print(f"Successfully inserted {len(rows_to_insert)} rows into {table_id}")

    print(f"File {file_name} processed.")

