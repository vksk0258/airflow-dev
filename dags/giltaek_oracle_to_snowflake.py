import os
import csv
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import pendulum
from datetime import datetime
import pandas as pd

# 공유 디렉토리 설정
SHARED_DIR = './'
#os.makedirs(SHARED_DIR, exist_ok=True)
FILE_PATH = os.path.join(SHARED_DIR, 'bank_data.csv')

def extract_from_oracle():
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
    df_transformed.to_csv("./table.csv", header=False)
    print("test2")
    snowflake_hook = SnowflakeHook(snowflake_conn_id='Snow _itsmart')
    connection = snowflake_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute("PUT file://./bank_data.csv @bank_stage")

    print(f"snow put")
    # 데이터 로드
    cursor.execute("""
            COPY INTO FINANCIAL_SC
            FROM @bank_stage/bank_data.csv
            FILE_FORMAT = (TYPE = 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY='"',  SKIP_HEADER = 1)
        """)

    print(f"snow copy")
    connection.close()
    cursor.close()
    os.remove(FILE_PATH)

with DAG(
    dag_id="giltaek_oracle_to_snowflake",
    # 이덱은 매일 6시 30분에 시작
    schedule="30 6 * * *",
    # 덱이 언제 부터 돌지 TZ=타임존
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"),
    # 캐치업 배치 중간에 누락된 구간을 돌릴지 말지
    # 1월 1일부터 3월 1일까지 누락된 덱을 한번에 돌아가게 된다 3월 1일에 왠만하면 false
    catchup=False,
    tags=["옵션", "태그"]
) as dag:
    extract_task = PythonOperator(
        task_id='extract_from_oracle',
        python_callable=extract_from_oracle
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )

    extract_task >> transform_task
