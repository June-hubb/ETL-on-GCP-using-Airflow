from airflow import models
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.python_operator import PythonOperator
from google.cloud import storage
import pandas as pd
import json

def transform_and_load_to_bq(**kwargs):
    # Read CSV file from GCS
    client = storage.Client()
    bucket_name = 'bucket name'
    blob_name = 'path to csv'
    bucket = client.get_bucket(bucket_name)
    blob = bucket.get_blob(blob_name)

    # Read CSV content
    content = blob.download_as_text()
    df = pd.read_csv(content)
    df=df.drop_duplicates().reset_index(drop=True)
    #Transformation
    #dropping missing data
    df.drop(df[df["MISSING_DATA"]==True].index, axis=0, inplace=True)
    # Convert the polyline string to a list
    df['POLYLINE'] = df['POLYLINE'].apply(json.loads)
    # Get the start and end points
    df['start_point'] = df['POLYLINE'].apply(lambda x: x[0] if x else 'NA')
    df['end_point'] = df['POLYLINE'].apply(lambda x: x[-1] if x else 'NA')
    df['start_point_longitude'] = df['start_point'].apply(lambda x: x[0] if x else 'NA')
    df['start_point_latitude'] = df['start_point'].apply(lambda x: x[1] if x else 'NA') 
    df['end_point_longitude'] = df['end_point'].apply(lambda x: x[0] if x else 'NA')
    df['end_point_latitude'] = df['end_point'].apply(lambda x: x[1] if x else 'NA')
    #Extract pick up time
    df['pickup_datetime'] = pd.to_datetime(df['TIMESTAMP'], unit='s')   
    columns_to_drop = ['MISSING_DATA', 'POLYLINE','start_point','end_point','TIMESTAMP','ORIGIN_STAND','ORIGIN_CALL','TRIP_ID']
    df.drop(columns=columns_to_drop, inplace=True)
    df['trip_id']=df.index
    datetime_dim = df[['pickup_datetime']]
    datetime_dim['pick_hour'] = datetime_dim['pickup_datetime'].dt.hour
    datetime_dim['pick_day'] = datetime_dim['pickup_datetime'].dt.day
    datetime_dim['pick_month'] = datetime_dim['pickup_datetime'].dt.month
    datetime_dim['pick_year'] = datetime_dim['pickup_datetime'].dt.year
    datetime_dim['pick_weekday'] = datetime_dim['pickup_datetime'].dt.weekday
    datetime_dim['datetime_id'] = datetime_dim.index
    day_type_code={
    "A":"Normal Day",
    "B":"Holiday",
    "C":"Day before Holiday"
    }
    day_type_dim=df[['DAY_TYPE']].reset_index(drop=True)
    day_type_dim['day_type_id']=day_type_dim.index
    day_type_dim['day_type_name']=day_type_dim['DAY_TYPE'].map(day_type_code)
    call_type_code={
    "A":"Central",
    "B":"Taxi driver or Stand",
    "C":"Others"
    }
    call_type_dim=df[['CALL_TYPE']].reset_index(drop=True)
    call_type_dim['call_type_id']=call_type_dim.index
    call_type_dim['call_type_name']=call_type_dim['CALL_TYPE'].map(call_type_code)
    fact_table = df.merge(datetime_dim, left_on='trip_id', right_on='datetime_id') \
             .merge(day_type_dim, left_on='trip_id', right_on='day_type_id') \
             .merge(call_type_dim, left_on='trip_id', right_on='call_type_id') \
             [['trip_id', 'datetime_id', 'day_type_id','call_type_id']]


    # Write to BigQuery
    bigquery_hook = BigQueryHook()

    # Write fact table
    fact_table.to_gbq('your_project.your_dataset.fact_table', bigquery_hook.project_id, if_exists='replace')

    # Write dimension tables
    call_type_dim.to_gbq('your_project.your_dataset.call_type_dim', bigquery_hook.project_id, if_exists='replace')
    day_type_dim.to_gbq('your_project.your_dataset.day_type_dim', bigquery_hook.project_id, if_exists='replace')
    datetime_dim.to_gbq('your_project.your_dataset.datetime_dim', bigquery_hook.project_id, if_exists='replace')

# Define Airflow DAG
with models.DAG(
        'gcs_to_bigquery',
        schedule_interval='@daily',
        default_args={'owner': 'airflow'},
        catchup=False) as dag:

    transform_and_load_task = PythonOperator(
        task_id='transform_and_load_to_bq',
        python_callable=transform_and_load_to_bq,
    )

    transform_and_load_task
