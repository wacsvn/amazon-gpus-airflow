#dag - directed acyclic graph

#tasks : 1) fetch amazon data (extract) 2) clean data (transform) 3) create and store data in table on postgres (load)
#operators : Python Operator and PostgresOperator
#hooks - allows connection to postgres
#dependencies

from datetime import datetime, timedelta
from airflow import DAG
import requests
import pandas as pd
from bs4 import BeautifulSoup
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

#1) fetch amazon data (extract) 2) clean data (transform)

headers = {
    "Referer": 'https://www.amazon.com/',
    "Sec-Ch-Ua": "Not_A Brand",
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": "macOS",
    'User-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'
}


def get_amazon_data_gpus(num_gpus, ti):
    # Base URL of the Amazon search results for GPUs
    base_url = f"https://www.amazon.com/s?k=graphics+cards+gpu"

    gpus = []
    seen_titles = set()  # To keep track of seen titles

    page = 1

    while len(gpus) < num_gpus:
        url = f"{base_url}&page={page}"
        
        # Send a request to the URL
        response = requests.get(url, headers=headers)
        
        # Check if the request was successful
        if response.status_code == 200:
            # Parse the content of the request with BeautifulSoup
            soup = BeautifulSoup(response.content, "html.parser")
            
            # Find GPU containers (you may need to adjust the class names based on the actual HTML structure)
            gpu_containers = soup.find_all("div", {"class": "s-result-item"})
            
            # Loop through the GPU containers and extract data
            for gpu in gpu_containers:
                title = gpu.find("span", {"class": "a-text-normal"})
                brand = gpu.find("a", {"class": "a-size-base"})
                price = gpu.find("span", {"class": "a-price-whole"})
                rating = gpu.find("span", {"class": "a-icon-alt"})
                
                if title and brand and price and rating:
                    gpu_title = title.text.strip()
                    
                    # Check if title has been seen before
                    if gpu_title not in seen_titles:
                        seen_titles.add(gpu_title)
                        gpus.append({
                            "Title": gpu_title,
                            "Brand": brand.text.strip(),
                            "Price": price.text.strip(),
                            "Rating": rating.text.strip(),
                        })
            
            # Increment the page number for the next iteration
            page += 1
        else:
            print("Failed to retrieve the page")
            break

    # Limit to the requested number of GPUs
    gpus = gpus[:num_gpus]
    
    # Convert the list of dictionaries into a DataFrame
    df = pd.DataFrame(gpus)
    
    # Remove duplicates based on 'Title' column
    df.drop_duplicates(subset="Title", inplace=True)
    
    # Push the DataFrame to XCom
    ti.xcom_push(key='gpu_data', value=df.to_dict('records'))

#3) create and store data in table on postgres (load)
    
def insert_gpu_data_into_postgres(ti):
    gpu_data = ti.xcom_pull(key='gpu_data', task_ids='fetch_gpu_data')
    if not gpu_data:
        raise ValueError("No GPU data found")

    postgres_hook = PostgresHook(postgres_conn_id='books_connection')
    insert_query = """
    INSERT INTO gpus (title, brand, price, rating)
    VALUES (%s, %s, %s, %s)
    """
    for gpu in gpu_data:
        postgres_hook.run(insert_query, parameters=(gpu['Title'], gpu['Brand'], gpu['Price'], gpu['Rating']))


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'fetch_and_store_amazon_gpus',
    default_args=default_args,
    description='A simple DAG to fetch GPU data from Amazon and store it in Postgres',
    schedule_interval=timedelta(days=1),
)

#operators : Python Operator and PostgresOperator
#hooks - allows connection to postgres


fetch_gpu_data_task = PythonOperator(
    task_id='fetch_gpu_data',
    python_callable=get_amazon_data_gpus,
    op_args=[50],  # Number of GPUs to fetch
    dag=dag,
)

create_table_task = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='books_connection',
    sql="""
    CREATE TABLE IF NOT EXISTS gpus (
        id SERIAL PRIMARY KEY,
        title TEXT NOT NULL,
        brand TEXT,
        price TEXT,
        rating TEXT
    );
    """,
    dag=dag,
)

insert_gpu_data_task = PythonOperator(
    task_id='insert_gpu_data',
    python_callable=insert_gpu_data_into_postgres,
    dag=dag,
)

#dependencies

fetch_gpu_data_task >> create_table_task >> insert_gpu_data_task