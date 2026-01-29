import airflow
from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor # for http requests
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator



from datetime import datetime, timedelta
from pendulum import date # more user-friendly than datetime
import json 
import pandas as pd
import requests
from pandas import json_normalize


# Grab current date
current_date = datetime.today().strftime('%Y-%m-%d')

# Default settings for all the dags in the pipeline
default_args = {

    "owner": "sarah", 
    "start_date" : datetime(2024,8,1),
    "retries" : 1,
    "retry_delay": timedelta(minutes=5)

}

def process_data(ti):
    extracted_data = ti.xcom_pull(task_ids ='extract_data', key= 'return_value')
    data = extracted_data['rates']
    # creating a dataframe
    processed_data =  pd.DataFrame(columns=['rate','symbol'])
    temp_df = dict()

    for symbol,rate in data.items():
        temp_df = json_normalize(
            {
                'rate': float(rate),
                'symbol' :symbol 
            }
        )

        processed_data = processed_data._append(temp_df)

    # the tmp folder exists in the airflow-worker container , to access enter its bash 
    processed_data.to_csv('./data/processed_data.csv',index = None,header =False)

def load_data():
  postgres_hook = PostgresHook(postgres_conn_id = "postgres_connection")
  
  postgres_hook.copy_expert(
    sql="COPY rates FROM ./data/processed_data.csv WITH DELIMITER as ','"
     
  )



with DAG(
    dag_id = 'forex_pipeline_v7',
    default_args=default_args,
    schedule_interval=timedelta(minutes=2), 
    catchup=False
    
)as dag:

    # TASK 1 - Check if the API is available
    is_api_available = HttpSensor(
        task_id='is_api_available',
        method='GET', # the http method to make the request
        http_conn_id='is_api_available',
        endpoint= current_date + '?access_key=63646631d0ca8724f3a2c6c30e0a5891',
        response_check= lambda response: 'EUR' in response.text,
        poke_interval = 5
    )

    # TASK 2 : create a table
    create_table = PostgresOperator(
        task_id ='create_table',
        postgres_conn_id = 'postgres_connection',
        sql = '''
            create table if not exists rates(
                rate float not null,
                symbol text not null
            );
        
        '''
    )

    # TASK 3 : extracting data into a json object stored in xcom
    extract_data = SimpleHttpOperator(
        task_id ='extract_data',
        http_conn_id = 'is_api_available',
        method ='GET',
        endpoint= current_date + '?access_key=fa8e54c9aa574e632931e3684cd8fa29',
        response_filter = lambda response: json.loads(response.text),
        log_response = True
    )

    # TASK 4: transforming the data into a pd and exporting it into a csv file
    transform_data = PythonOperator(
        task_id = 'transform_data',
        python_callable= process_data

    )

    # TASK 5 : loading into the database
    load_data = PythonOperator(
        task_id ="load_data",
        python_callable= load_data
    )

    # dependencies
    is_api_available >> create_table >> extract_data >> transform_data >> load_data
