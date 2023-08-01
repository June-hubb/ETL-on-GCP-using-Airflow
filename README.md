# ETL-on-GCP-using-Airflow
ETL using Airflow (hosted on Google Compute Engine) on taxi data and visualizing it on Looker Studio

## Technologies Used
1) Python
2) Airflow
3) Compute Engine
4) Google Cloud Storage
5) BigQuery
6) Looker Studio

## Description 
Taxi data from Kaggle is cleaned and processed using Pandas library which is split into fact and dimension tables (data frames) in Jupyter Notebook for reference before orchestrating it using Airflow. Airflow is hosted on the compute engine on Google Cloud. Data is present in Google Storage which is extracted and transformed (using pandas data frame) and then loaded into BigQuery. The cleaned data is then visualized on Looker Studio. 

## Dataset
https://www.kaggle.com/datasets/crailtap/taxi-trajectory

## Architecture Diagram
<img width="713" alt="image" src="https://github.com/June-hubb/ETL-on-GCP-using-Airflow/assets/72768636/c632a32b-de32-4b59-9eb4-c85b9e80af4d">

## Data Model

<img width="637" alt="image" src="https://github.com/June-hubb/ETL-on-GCP-using-Airflow/assets/72768636/124ce92e-3dd7-4c15-822b-3497c492eaaf">

