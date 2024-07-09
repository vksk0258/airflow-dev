from airflow import DAG
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'mason',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 28),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'snowflake_etl_test_dag',
    default_args=default_args,
    schedule_interval='30 10 * * *',
    catchup=False
)

def extract_data():
    oracle_hook = OracleHook(oracle_conn_id='Ora_mason')
    sql = 'SELECT * FROM member'
    connection = oracle_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql)
    results = cursor.fetchall()
    cursor.close()
    connection.close()
    return results

def sort_data(ti):
    extracted_data = ti.xcom_pull(task_ids='extract_data')
    sorted_data = sorted(extracted_data, key=lambda x: x[0])
    return sorted_data

def load_data_to_snowflake(**kwargs):
    snowflake_hook = SnowflakeHook(snowflake_conn_id='Snow_mason')
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='sort_data', key='return_value')
    
    connection = snowflake_hook.get_conn()
    cursor = connection.cursor()
    
    insert_query = "INSERT INTO t_member VALUES ({})".format(
        ','.join(['%s' for _ in range(len(data[0]))])
    )
    
    cursor.executemany(insert_query, data)
    connection.commit()
    cursor.close()

start_task = DummyOperator(task_id='start', dag=dag)

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

sort_task = PythonOperator(
    task_id='sort_data',
    python_callable=sort_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data_to_snowflake',
    python_callable=load_data_to_snowflake,
    provide_context=True,
    dag=dag,
)

end_task = DummyOperator(task_id='end', dag=dag)

start_task >> extract_task >> sort_task >> load_task >> end_task
