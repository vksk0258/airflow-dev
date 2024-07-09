from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine

default_args = {
    'owner': 'mason',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'snowflake_transform_test2',
    default_args=default_args,
    description='Extract from Oracle, transform, and load to Snowflake',
    schedule_interval='@daily',
)

def extract_from_oracle(**kwargs):
    oracle_hook = OracleHook(oracle_conn_id='Ora_mason')
    sql = """
    SELECT * FROM (SELECT ENTITY_NAME, CITY, STATE_ABBREVIATION, VARIABLE_NAME, YEAR, MONTH, VALUE, UNIT, DEFINITION
    FROM MASON.FINANCIAL_ENTITY_ANNUAL_TIME_SERIES) WHERE ROWNUM <= 100
    """
    connection = oracle_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql)
    data = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(data, columns=column_names)
    cursor.close()
    connection.close()
    return df.to_dict('records')

def transform_data(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract_from_oracle')
    df = pd.DataFrame(data)
    
    df_pivot = df.pivot_table(
        index=['ENTITY_NAME', 'CITY', 'STATE_ABBREVIATION', 'YEAR'],
        columns='VARIABLE_NAME',
        values='VALUE',
        aggfunc='first'
    ).reset_index()
    
    df_pivot.columns = [str(col) if isinstance(col, tuple) else col for col in df_pivot.columns]
    df_transformed = df_pivot.rename_axis(None, axis=1).reset_index(drop=True)
    print(df_transformed)
    return df_transformed.to_dict('records')

def load_to_snowflake(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='transform_data')
    df = pd.DataFrame(data)

    df.to_csv("./table.csv", header=False)

    snowflake_hook = SnowflakeHook(snowflake_conn_id='Snow_mason')
    connection = snowflake_hook.get_conn()
    cursor = connection.cursor()

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS MASON.FINANCIAL_ENTITY_ANNUAL_TIME_SERIES_TRANSFORMED (
        ENTITY_NAME STRING,
        CITY STRING,
        STATE_ABBREVIATION STRING,
        YEAR NUMBER,
        "Total Assets" NUMBER,
        "Total Securities" NUMBER,
        "Total deposits" NUMBER,
        "% Insured (Estimated)" NUMBER,
        "All Real Estate Loans" NUMBER
    );
    """

    cursor.execute(create_table_sql)

    connection.commit()

    stage_name = 'bank_stage'
    cursor.execute("PUT file://./table.csv")

    print(f"snow put")
    # 데이터 로드
    cursor.execute("""
        COPY INTO FINANCIAL_SC
        FROM @bank_stage/table.csv
        FILE_FORMAT = (TYPE = 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY='"')
    """)

    print(f"snow copy")  
    cursor.close()
 


extract_task = PythonOperator(
    task_id='extract_from_oracle',
    python_callable=extract_from_oracle,
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
    task_id='load_to_snowflake',
    python_callable=load_to_snowflake,
    provide_context=True,
    dag=dag,
)

extract_task >> transform_task >> load_task
