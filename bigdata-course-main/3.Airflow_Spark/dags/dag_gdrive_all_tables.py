from airflow import DAG
import pandas as pd
from airflow.hooks.base import BaseHook
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from os import getenv
from sqlalchemy import create_engine
import requests
from datetime import datetime

DAG_DEFAULT_ARGS = {'start_date': datetime(2020, 1, 1), 'depends_on_past': False}
DEFAULT_POSTGRES_CONN_ID = "postgres_default"
AIRFLOW_HOME = getenv('AIRFLOW_HOME', '/opt/airflow')

DAG_ID = "LOAD_DATA_GDRIVE_ALL_TABLES"
schedule = "@hourly"



def load_from_gdrive(file_path: str, table_name: str, schema: str = "main_datamart", conn_id: str = None) -> None:

    conn_object = BaseHook.get_connection(conn_id or DEFAULT_POSTGRES_CONN_ID)
    # extra = conn_object.extra_dejson
    jdbc_url = f"postgresql://{conn_object.login}:{conn_object.password}@" \
               f"{conn_object.host}:{conn_object.port}/{conn_object.schema}"
 
    from google_drive_downloader import GoogleDriveDownloader as gdd
    
    try:
        gdd.download_file_from_google_drive(file_id='1uEtRcrxRsbfT7HrnuCTL7FZ80Ip3eICZ',
                                            dest_path= f'{AIRFLOW_HOME}/example/Customers.csv',
                                            unzip=True)
        gdd.download_file_from_google_drive(file_id='1IpImNbqSIBfYeZh-NhaOxi5YrnPSqSsx',
                                            dest_path=f'{AIRFLOW_HOME}/example/Payment.csv',
                                            unzip=True)
        gdd.download_file_from_google_drive(file_id='1t_nLdo8iyCX23PAZRiYwpKv5Z_iXzSVe',
                                            dest_path=f'{AIRFLOW_HOME}/example/products.csv',
                                            unzip=True)
        gdd.download_file_from_google_drive(file_id='1k5lmPIaH830TrXhslXO9XNpmfEcWhPbL',
                                            dest_path=f'{AIRFLOW_HOME}/example/costed_events_0.csv',
                                            unzip=True)
        gdd.download_file_from_google_drive(file_id='1eMTCSy9-945AhbWRd2DsyC2CZxN5cZDy',
                                            dest_path=f'{AIRFLOW_HOME}/example/charges.csv',
                                            unzip=True)
        gdd.download_file_from_google_drive(file_id='1dSyu3EqR5V0UyPmi3PzqoVSlEtsGi_MB',
                                            dest_path=f'{AIRFLOW_HOME}/example/business_product_instances.csv',
                                            unzip=True)
        print(f'cюда...{AIRFLOW_HOME}')
    except:
        print('Ошибка!')

    df = pd.read_csv(f'{AIRFLOW_HOME}/example/Customers.csv')
    engine = create_engine(jdbc_url)
    #df.to_csv(f'{AIRFLOW_HOME}/example/Customers11.csv')
    df.to_sql(table_name, engine, schema=schema, if_exists="replace")

    df = pd.read_csv(f'{AIRFLOW_HOME}/example/Payments.csv')
    engine = create_engine(jdbc_url)
    # df.to_csv(f'{AIRFLOW_HOME}/example/Customers11.csv')
    df.to_sql(table_name, engine, schema=schema, if_exists="replace")

    df = pd.read_csv(f'{AIRFLOW_HOME}/example/products.csv')
    engine = create_engine(jdbc_url)
    # df.to_csv(f'{AIRFLOW_HOME}/example/Customers11.csv')
    df.to_sql(table_name, engine, schema=schema, if_exists="replace")

    df = pd.read_csv(f'{AIRFLOW_HOME}/example/costed_events_0.csv')
    engine = create_engine(jdbc_url)
    # df.to_csv(f'{AIRFLOW_HOME}/example/Customers11.csv')
    df.to_sql(table_name, engine, schema=schema, if_exists="replace")

    df = pd.read_csv(f'{AIRFLOW_HOME}/example/charges.csv')
    engine = create_engine(jdbc_url)
    # df.to_csv(f'{AIRFLOW_HOME}/example/Customers11.csv')
    df.to_sql(table_name, engine, schema=schema, if_exists="replace")

    df = pd.read_csv(f'{AIRFLOW_HOME}/example/business_product_instances.csv')
    engine = create_engine(jdbc_url)
    # df.to_csv(f'{AIRFLOW_HOME}/example/Customers11.csv')
    df.to_sql(table_name, engine, schema=schema, if_exists="replace")




with DAG(dag_id=DAG_ID,
         description='Dag to transfer data from csv to postgres [version 1.0]',
         schedule_interval=schedule,
         default_args=DAG_DEFAULT_ARGS,
         is_paused_upon_creation=True,
         max_active_runs=1,
         catchup=False
         ) as dag:
    start_task = DummyOperator(task_id='START', dag=dag)
    end_task = DummyOperator(task_id='END', dag=dag)

    customer_table_name = "customer"
    payments_table_name = "payments"
    products_table_name = "products"
    costed_events_name = "costed_events"
    charges_table_name = "charges"
    business_product_instances_table_name = "business_product_instances"


    load_customers_raw_task = PythonOperator(dag=dag,
                                            task_id=f"{DAG_ID}.RAW.{payments_table_name}",
                                            python_callable=load_from_gdrive,
                                            op_kwargs={
                                                "file_path": f"{AIRFLOW_HOME}/example/Customers.csv",
                                                "table_name": customer_table_name,
                                                "conn_id": "raw_postgres"
                                            }
                                            )
    load_payments_raw_task = PythonOperator(dag=dag,
                                             task_id=f"{DAG_ID}.RAW.{payments_table_name}",
                                             python_callable=load_from_gdrive,
                                             op_kwargs={
                                                 "file_path": f"{AIRFLOW_HOME}/example/Payments.csv",
                                                 "table_name": payments_table_name,
                                                 "conn_id": "raw_postgres"
                                             }
                                             )
    load_products_raw_task = PythonOperator(dag=dag,
                                             task_id=f"{DAG_ID}.RAW.{payments_table_name}",
                                             python_callable=load_from_gdrive,
                                             op_kwargs={
                                                 "file_path": f"{AIRFLOW_HOME}/example/products.csv",
                                                 "table_name": products_table_name,
                                                 "conn_id": "raw_postgres"
                                             }
                                             )
    load_costed_events_raw_task = PythonOperator(dag=dag,
                                             task_id=f"{DAG_ID}.RAW.{payments_table_name}",
                                             python_callable=load_from_gdrive,
                                             op_kwargs={
                                                 "file_path": f"{AIRFLOW_HOME}/example/costed_events_0.csv",
                                                 "table_name": costed_events_name,
                                                 "conn_id": "raw_postgres"
                                             }
                                             )
    load_charges_raw_task = PythonOperator(dag=dag,
                                             task_id=f"{DAG_ID}.RAW.{payments_table_name}",
                                             python_callable=load_from_gdrive,
                                             op_kwargs={
                                                 "file_path": f"{AIRFLOW_HOME}/example/charges.csv",
                                                 "table_name": charges_table_name,
                                                 "conn_id": "raw_postgres"
                                             }
                                             )
    load_business_product_instances_raw_task = PythonOperator(dag=dag,
                                             task_id=f"{DAG_ID}.RAW.{payments_table_name}",
                                             python_callable=load_from_gdrive,
                                             op_kwargs={
                                                 "file_path": f"{AIRFLOW_HOME}/example/business_product_instances.csv",
                                                 "table_name": business_product_instances_table_name,
                                                 "conn_id": "raw_postgres"
                                             }
                                             )

    #start_task >> load_customers_raw_task >> customer_totals_datamart_task >> end_task

    start_task >> [load_customers_raw_task,load_payments_raw_task,load_products_raw_task,load_costed_events_raw_task,load_charges_raw_task,load_business_product_instances_raw_task] >> end_task

    # 1) DAG for raw layer, separated postgres
    # 2) DAG for datamart layer and dependency resolving
    # 3) Data from external source (e.g. google drive)