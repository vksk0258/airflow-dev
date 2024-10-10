from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import pandas as pd
import pprint
import time

def df_to_sql():
    start = time.time()
    oracle_hook = OracleHook(oracle_conn_id='ora_mason')
    sql = """
    SELECT * FROM (SELECT ENTITY_NAME, CITY, STATE_ABBREVIATION, VARIABLE_NAME, YEAR, MONTH, VALUE, UNIT, DEFINITION
    FROM MASON.FINANCIAL_ENTITY_ANNUAL_TIME_SERIES)
    """
    connection = oracle_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql)

    point1 = time.time()
    pprint.pprint(f"SQL를 통해 데이터를 가져오는 시간: {point1 - start} sec")
    data = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(data, columns=column_names)
    cursor.close()
    connection.close()
    df_pivot = df.pivot_table(
        index=['ENTITY_NAME', 'CITY', 'STATE_ABBREVIATION', 'YEAR'],
        columns='VARIABLE_NAME',
        values='VALUE',
        aggfunc='first'
    ).reset_index()
    df_pivot.columns = [str(col) if isinstance(col, tuple) else col for col in df_pivot.columns]
    df_transformed = df_pivot.rename_axis(None, axis=1).reset_index(drop=True)

    point2 = time.time()
    pprint.pprint(f"Pandas Pivot을 활용해 데이터를 트랜스폼 하는 시간: {point2 - point1} sec")
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snow_itsmart')
    connection = snowflake_hook.get_conn()
    df_transformed.to_sql(name='AIRFLOW.FINANCIAL_SC', con=connection, if_exists='replace', index=False)

    point3 = time.time()
    pprint.pprint(f"Airflow 서버에 csv파일로 저장하는 시간: {point3 - point2} sec")


def extract_from_oracle_all():
    start = time.time()
    oracle_hook = OracleHook(oracle_conn_id='ora_mason')
    sql = """
    
    
    SELECT * FROM (SELECT ENTITY_NAME, CITY, STATE_ABBREVIATION, VARIABLE_NAME, YEAR, MONTH, VALUE, UNIT, DEFINITION
    FROM MASON.FINANCIAL_ENTITY_ANNUAL_TIME_SERIES)
    """
    connection = oracle_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql)

    point1 = time.time()
    pprint.pprint(f"SQL를 통해 데이터를 가져오는 시간: {point1 - start} sec")
    data = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(data, columns=column_names)
    cursor.close()
    connection.close()
    df_pivot = df.pivot_table(
        index=['ENTITY_NAME', 'CITY', 'STATE_ABBREVIATION', 'YEAR'],
        columns='VARIABLE_NAME',
        values='VALUE',
        aggfunc='first'
    ).reset_index()
    df_pivot.columns = [str(col) if isinstance(col, tuple) else col for col in df_pivot.columns]
    df_transformed = df_pivot.rename_axis(None, axis=1).reset_index(drop=True)

    point2 = time.time()
    pprint.pprint(f"Pandas Pivot을 활용해 데이터를 트랜스폼 하는 시간: {point2 - point1} sec")
    df_transformed.to_csv("/opt/airflow/plugins/files/bank_data_all.csv", header=False)

    point3 = time.time()
    pprint.pprint(f"Airflow 서버에 csv파일로 저장하는 시간: {point3 - point2} sec")


def load_data_all():
    point4 = time.time()
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snow_itsmart')
    connection = snowflake_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute("PUT file:///opt/airflow/plugins/files/bank_data_all.csv @bank_stage")

    point5 = time.time()
    pprint.pprint(f"PUT을 활용해서 CSV파일을 스테이지에 업로드 하는 시간: {point5 - point4} sec")
    # 데이터 로드
    cursor.execute("""
            COPY INTO FINANCIAL_SC
            FROM @bank_stage/bank_data_all.csv
            FILE_FORMAT = (TYPE = 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY='"',  SKIP_HEADER = 1)
        """)

    connection.close()
    cursor.close()

    point6 = time.time()
    pprint.pprint(f"COPY INTO 명령어를 날려 CSV파일을 테이블로 저장하는 시간: {point6 - point5} sec")

def extract_from_oracle(**kwargs):
    oracle_hook = OracleHook(oracle_conn_id='ora_mason')
    sql = """
    SELECT * FROM (SELECT ENTITY_NAME, CITY, STATE_ABBREVIATION, VARIABLE_NAME, YEAR, MONTH, VALUE, UNIT, DEFINITION
    FROM MASON.FINANCIAL_ENTITY_ANNUAL_TIME_SERIES) WHERE ROWNUM <= 100
    """
    connection = oracle_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql)
    ti = kwargs['ti']
    data = cursor.fetchall()
    ti.xcom_push(key="cursor_fetchall", value=data)
    column_names = [desc[0] for desc in cursor.description]
    ti.xcom_push(key="cursor_description", value=column_names)
    df = pd.DataFrame(data, columns=column_names)
    ti.xcom_push(key="df", value=df)
    cursor.close()
    connection.close()

def transform_data(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(key="df", task_ids='extract_from_oracle')
    # df = pd.DataFrame(data)

    df_pivot = df.pivot_table(
        index=['ENTITY_NAME', 'CITY', 'STATE_ABBREVIATION', 'YEAR'],
        columns='VARIABLE_NAME',
        values='VALUE',
        aggfunc='first'
    ).reset_index()

    df_pivot.columns = [str(col) if isinstance(col, tuple) else col for col in df_pivot.columns]
    df_transformed = df_pivot.rename_axis(None, axis=1).reset_index(drop=True)

    pprint.pprint(df_transformed)
    df_transformed.to_csv("/opt/airflow/plugins/files/bank_data.csv", header=False)

    snowflake_hook = SnowflakeHook(snowflake_conn_id='snow_itsmart')
    connection = snowflake_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute("PUT file:///opt/airflow/plugins/files/bank_data.csv @bank_stage")
    # 데이터 로드
    cursor.execute("""
            COPY INTO FINANCIAL_SC
            FROM @bank_stage/bank_data.csv
            FILE_FORMAT = (TYPE = 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY='"',  SKIP_HEADER = 1)
        """)

    connection.close()
    cursor.close()




def extract_from_oracle_dongchan():
    start = time.time()
    oracle_hook = OracleHook(oracle_conn_id='ora_mason')
    sql = """
    
    
    SELECT * FROM (SELECT ENTITY_NAME, CITY, STATE_ABBREVIATION, VARIABLE_NAME, YEAR, MONTH, VALUE, UNIT, DEFINITION
    FROM MASON.FINANCIAL_ENTITY_ANNUAL_TIME_SERIES)
    """
    connection = oracle_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql)

    point1 = time.time()
    pprint.pprint(f"SQL를 통해 데이터를 가져오는 시간: {point1 - start} sec")
    data = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(data, columns=column_names)
    cursor.close()
    connection.close()
   
    point2 = time.time()
    pprint.pprint(f"Pandas Pivot을 활용해 데이터를 트랜스폼 하는 시간: {point2 - point1} sec")
    df.to_csv("/opt/airflow/plugins/files/bank_data_all.csv", header=False)

    point3 = time.time()
    pprint.pprint(f"Airflow 서버에 csv파일로 저장하는 시간: {point3 - point2} sec")

def load_data():
    point4 = time.time()
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snow_dongchan')
    connection = snowflake_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute("PUT file:///opt/airflow/plugins/files/bank_data_all.csv @bank_stage")

    point5 = time.time()
    pprint.pprint(f"PUT을 활용해서 CSV파일을 스테이지에 업로드 하는 시간: {point5 - point4} sec")
    # 데이터 로드
    cursor.execute("""
            COPY INTO FINANCIAL_TEST
            FROM @bank_stage/bank_data_all.csv
            FILE_FORMAT = (TYPE = 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY='"',  SKIP_HEADER = 1)
        """)

    connection.close()
    cursor.close()

    point6 = time.time()
    pprint.pprint(f"COPY INTO 명령어를 날려 CSV파일을 테이블로 저장하는 시간: {point6 - point5} sec")
