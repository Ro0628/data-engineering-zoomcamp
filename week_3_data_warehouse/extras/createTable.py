
import os
from google.cloud import bigquery
import pyarrow.parquet as pq

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] ='$HOME/.config/gcloud/application_default_credentials.json'

# switch out the bucketname
bucket_name = os.environ.get("GCP_GCS_BUCKET", "dtc_data_lake_my-rides-ro")

# Set the dataset and table names
dataset_id = 'nytaxi'
file_names  = []
gcs_uris = []
year = '2020'
service = 'green'

for i in range(12):
        
    # sets the month part of the file_name string
    month = '0'+str(i+1)
    month = month[-2:]

    # parquet file_name 
    file_name = service + '_tripdata_' + year + '-' + month + '.parquet'
    
    gcs_uri=f"gs://{bucket_name}/{service}/{file_name}"
    
    # Read the data from the Parquet file
    table = pq.read_table(gcs_uri)
    df = table.to_pandas()

    # Get the schema from the dataframe
    bq_schema = []
    for col_name, dtype in df.dtypes.iteritems():
        print(col_name + dtype.name)
        bq_field = bigquery.SchemaField(col_name, dtype.name)
        bq_schema.append(bq_field)

    # Create a BigQuery client
    client = bigquery.Client()

    # Create the BigQuery dataset
    dataset = bigquery.Dataset(bigquery.dataset.DatasetReference(client.project, dataset_id))
    dataset = client.create_dataset(dataset)

    # Load the data into the table
    load_job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=bq_schema,
    )
    load_job = client.load_table_from_uri(
        gcs_uri,
        bigquery.dataset.DatasetReference(client.project, dataset_id).table(table_id),
        job_config=load_job_config,
    )
    
     # Wait for the load job to complete
    load_job.result()


    # Check if the table has been created
    #table = client.get_table(bigquery.dataset.DatasetReference(client.project, dataset_id).table(table_id))
    print("Table created successfully: {}".format(table.table_id))
