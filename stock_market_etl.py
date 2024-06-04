from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import os

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate the DAG
dag = DAG(
    'stock_market_dag',
    default_args=default_args,
    description='A ETL process for stock market data',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Define the extract function
def extract_data(**kwargs):
    api_key = '6QKL0CN025S3X2YT'
    symbol = 'MSFT'
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}'
    response = requests.get(url)
    data = response.json()
    
    if "Time Series (Daily)" in data:
        return data["Time Series (Daily)"]
    else:
        raise ValueError("Failed to fetch data from Alpha Vantage")

# Define the transform function
def transform_data(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract_data')
    
    df = pd.DataFrame.from_dict(data, orient='index')
    df.index = pd.to_datetime(df.index)
    df.columns = [col.split('. ')[1] for col in df.columns]
    
    df = df.reset_index().rename(columns={'index': 'date'})
    df['date'] = df['date'].astype(str)  # Convert Timestamps to strings
    df.sort_values(by='date', ascending=True, inplace=True)
    
    return df.to_dict(orient='records')

# Define the load function
def load_data(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='transform_data')
    
    df = pd.DataFrame(data)
    
    # Generate a unique filename based on the current timestamp
    now = datetime.now()
    dt_string = now.strftime("%d%m%Y%H%M%S")
    filename = f'stock_data_{dt_string}.csv'
    output_path = os.path.join('data', filename)
    
    # Create the data directory if it doesn't exist
    os.makedirs('data', exist_ok=True)
    
    df.to_csv(output_path, index=False)

# Define the tasks
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
extract_task >> transform_task >> load_task
