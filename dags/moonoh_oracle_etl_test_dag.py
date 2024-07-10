from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 26),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'moonoh_oracle_etl_test_dag',
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

def load_data(ti):
    sorted_data = ti.xcom_pull(task_ids='sort_data')
    oracle_hook = OracleHook(oracle_conn_id='Ora_mason')
    connection = oracle_hook.get_conn()
    cursor = connection.cursor()
    insert_sql = 'INSERT INTO t_member (no, name, department, hiredate, salary, tel) VALUES (:1, :2, :3, :4, :5, :6)'
    cursor.executemany(insert_sql, sorted_data)
    connection.commit()
    cursor.close()
    connection.close()

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
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,
    dag=dag,
)

extract_task >> sort_task >> load_task
