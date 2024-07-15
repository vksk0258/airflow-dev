from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import pandas as pd

def extract_from_oracle():
    oracle_hook = OracleHook(oracle_conn_id='ora_mason')
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
    df_transformed.to_csv("./bank_data.csv", header=False)

    print('test1')
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snow_itsmart')
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