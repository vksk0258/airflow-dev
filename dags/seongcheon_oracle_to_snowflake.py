import os
import csv
from airflow import DAG
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# 공유 디렉토리 설정
SHARED_DIR = './'
#os.makedirs(SHARED_DIR, exist_ok=True)
FILE_PATH = os.path.join(SHARED_DIR, 'bank_data.csv')

def extract_data_from_oracle():
    oracle_hook = OracleHook(oracle_conn_id='Ora_mason')
    conn = oracle_hook.get_conn()
    cursor = conn.cursor()
    sql = """SELECT ENTITY_NAME
            ,CITY
            ,STATE_ABBREVIATION
            ,to_date(to_char(YEAR) || '-' || to_char(MONTH,'00') || '-' || '01', 'YYYY-MM-DD') AS "ML_DATE"
            ,sum(decode(VARIABLE_NAME, 'Total Assets', value)) as TotalAseets
            ,sum(decode(VARIABLE_NAME, 'Total deposits', value)) as Deposits
            ,sum(decode(VARIABLE_NAME, '% Insured (Estimated)', value * 100)) as Insured
            ,sum(decode(VARIABLE_NAME, 'All Real Estate Loans', value)) as Loans
            ,sum(decode(VARIABLE_NAME, 'Total Securities', value)) as Securities
         FROM FINANCIAL_ENTITY_ANNUAL_TIME_SERIES 
         GROUP BY ENTITY_NAME, CITY, STATE_ABBREVIATION, to_date(to_char(YEAR) || '-' || to_char(MONTH,'00') || '-' || '01', 'YYYY-MM-DD') 
         ORDER BY to_date(to_char(YEAR) || '-' || to_char(MONTH,'00') || '-' || '01', 'YYYY-MM-DD')"""
    cursor.execute(sql)
    result = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]


    try:
        # CSV 파일에 데이터 쓰기
        with open(FILE_PATH, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(column_names)
            writer.writerows(result)
        print(f"CSV 파일이 {FILE_PATH}에 성공적으로 작성되었습니다.")
    except Exception as e:
        print(f"CSV 파일 작성 중 오류 발생: {e}")
    
    
    cursor.close()
    conn.close()

def load_data_to_snowflake():
    print(f"snow시작")
    snowflake_hook = SnowflakeHook(snowflake_conn_id='Snow _itsmart')
    conn = snowflake_hook.get_conn()
    cursor = conn.cursor()
    print(f"snow시작")

    # Snowflake stage에 파일 업로드
    stage_name = 'bank_stage'
    cursor.execute("PUT file://./bank_data.csv @bank_stage")

    print(f"snow put")
    # 데이터 로드
    cursor.execute("""
        COPY INTO FINANCIAL_SC
        FROM @bank_stage/bank_data.csv
        FILE_FORMAT = (TYPE = 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY='"',  SKIP_HEADER = 1)
    """)

    print(f"snow copy")
    # 파일 삭제
    # cursor.execute(f"REMOVE @{stage_name}/bank_data.csv")
    cursor.close()
    conn.close()

    # 로컬 파일 삭제
    os.remove(FILE_PATH)

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
    dag_id='seongcheon_oracle_to_snowflake',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id='extract_data_from_oracle',
        python_callable=extract_data_from_oracle,
    )

    load_task = PythonOperator(
        task_id='load_data_to_snowflake',
        python_callable=load_data_to_snowflake,
    )

    extract_task >> load_task