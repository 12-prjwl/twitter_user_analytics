from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from azure.storage.blob import BlobServiceClient
import pandas as pd
import requests

selected_columns = ['username', 'name', 'follower_count', 'following_count', 'favourites_count', 'is_blue_verified', 'description', 'number_of_tweets', 'listed_count']
new_ids = ['RafaelNadal']

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 5),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

def fetch_new_user_data():
    old_df = pd.read_csv("path to ur dataset\\twitter.csv")
    new_df = add_users(new_ids, old_df)
    new_df = new_df[selected_columns]
    dataset = pd.concat([old_df, new_df], ignore_index=True)
    dataset.to_csv("path to ur new dataset\\new_twitter.csv", index=False)
    print("New user data fetched and added to the dataset.")

def add_users(new_ids, df):
    url = "https://twitter154.p.rapidapi.com/user/details"
    responses = []
    for id in new_ids:
        querystring = {"username": id}
        headers = {
            "X-RapidAPI-Key": "" ,# api key
            "X-RapidAPI-Host": "twitter154.p.rapidapi.com"
        }
        response = requests.get(url, headers=headers, params=querystring)
        result = response.json()
        responses.append(result)
    new_df = pd.DataFrame(responses)
    return new_df

def upload_to_blob():
    connection_string = "" 
    container_name = ""
    local_file_path = "path to ur local file\\new_twitter.csv" 
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    blob_container_client = blob_service_client.get_blob_client(container=container_name, blob="twitter_cloud1.csv")
    with open(local_file_path, "rb") as data:
        blob_container_client.upload_blob(data=data)
    print("File uploaded successfully to Azure Blob Storage.")

with DAG('twitter_data_processing',
         default_args=default_args,
         description='Process Twitter data and upload to Azure Blob Storage',
         schedule_interval='@once',
         catchup=False) as dag:

    fetch_new_user_data_task = PythonOperator(
        task_id='fetch_new_user_data',
        python_callable=fetch_new_user_data
    )

    upload_to_blob_task = PythonOperator(
        task_id='upload_to_blob',
        python_callable=upload_to_blob
    )

    fetch_new_user_data_task >> upload_to_blob_task

