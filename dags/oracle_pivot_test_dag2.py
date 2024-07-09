from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.operators.python import PythonOperator
import pandas as pd

# 기본 인자 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 6),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 정의
dag = DAG(
    'oracle_pivot_test_dag2',
    default_args=default_args,
    schedule_interval='30 10 * * *',
    catchup=False
)

# Source 데이터 가져오기
def extract_data():
    oracle_hook = OracleHook(oracle_conn_id='Ora_mason')
    sql = 'SELECT * FROM MASON.test_data'
    connection = oracle_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql)
    results = cursor.fetchall()
    columns = [col[0] for col in cursor.description]  # 컬럼 이름 가져오기
    cursor.close()
    connection.close()
    
    df = pd.DataFrame(results, columns=columns)
    return df.to_json()  # DataFrame을 JSON 문자열로 변환하여 반환

# 데이터 변환
def transform_data(**kwargs):
    ti = kwargs['ti']
    df_json = ti.xcom_pull(task_ids='extract_data')
    df = pd.read_json(df_json)  # JSON 문자열을 DataFrame으로 변환
    
    df_pivot = df.pivot_table(
        index=['ENTITY_NAME', 'CITY', 'STATE_ABBREVIATION', 'YEAR'],
        columns='VARIABLE_NAME',
        values='VALUE',
        aggfunc='first'
    ).reset_index()
    
    df_pivot = df_pivot.rename_axis(None, axis=1).reset_index(drop=True)
    return df_pivot.to_json()  # 변환된 DataFrame을 JSON 문자열로 반환

# Target에 적재
def load_data(**kwargs):
    ti = kwargs['ti']
    df_json = ti.xcom_pull(task_ids='transform_data')
    df_transformed = pd.read_json(df_json)  # JSON 문자열을 DataFrame으로 변환
    
    oracle_hook = OracleHook(oracle_conn_id='Ora_mason')
    connection = oracle_hook.get_conn()
    cursor = connection.cursor()

    insert_sql = '''
    INSERT INTO MASON.t_test_data (ENTITY_NAME, CITY, STATE_ABBREVIATION, YEAR, "All Real Estate Loans", "Total Assets", "Total Securities", "Total deposits", "% Insured (Estimated)")
    VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9)
    '''
    
    data = df_transformed.values.tolist()  # DataFrame을 리스트로 변환

    cursor.executemany(insert_sql, data)
    connection.commit()
    cursor.close()
    connection.close()

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,  # Airflow 2.0 이상에서는 불필요
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,  # Airflow 2.0 이상에서는 불필요
    dag=dag,
)

extract_task >> transform_task >> load_task
