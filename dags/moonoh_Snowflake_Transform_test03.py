from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import pandas as pd
import os
import logging
import tempfile

default_args = {
    'owner': 'mason',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 22),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'moonoh_snowflake_transform_test03',
    default_args=default_args,
    description='Extract from Oracle, transform, and load to Snowflake',
    schedule_interval='@daily',
)

def ensure_directory_exists(file_path):
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)
        logging.info(f"Created directory: {directory}")
    if not os.access(directory, os.W_OK):
        raise PermissionError(f"Directory is not writable: {directory}")

def extract_from_oracle(file_path, **kwargs):
    logging.info(f"Starting extract_from_oracle, file_path: {file_path}")
    ensure_directory_exists(file_path)

    oracle_hook = OracleHook(oracle_conn_id='Ora_mason')
    sql = """
    SELECT ENTITY_NAME, CITY, STATE_ABBREVIATION, VARIABLE_NAME, YEAR, MONTH, VALUE, UNIT, DEFINITION
    FROM MASON.FINANCIAL_ENTITY_ANNUAL_TIME_SERIES
    """
    df = oracle_hook.get_pandas_df(sql)
    logging.info(f"Extracted data: {df.head()}")
    df.to_json(file_path, orient='records')
    logging.info(f"Data saved to {file_path}")

def transform_data(input_path, output_path, **kwargs):
    logging.info(f"Starting transform_data, input_path: {input_path}, output_path: {output_path}")
    ensure_directory_exists(output_path)

    df = pd.read_json(input_path)
    df_pivot = df.pivot_table(
        index=['ENTITY_NAME', 'CITY', 'STATE_ABBREVIATION', 'YEAR'],
        columns='VARIABLE_NAME',
        values='VALUE',
        aggfunc='first'
    ).reset_index()
    df_pivot = df_pivot.rename_axis(None, axis=1).reset_index(drop=True)
    df_pivot.columns = df_pivot.columns.str.strip('"') 
    df_pivot.to_json(output_path, orient='records')
    logging.info(f"Transformed data saved to {output_path}")

def load_to_snowflake(file_path, **kwargs):
    logging.info(f"Starting load_to_snowflake, file_path: {file_path}")
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Input file does not exist: {file_path}")

    df = pd.read_json(file_path)
    df = df.where(pd.notnull(df), None)
    
    snowflake_hook = SnowflakeHook(snowflake_conn_id='Snow_mason')
    conn = snowflake_hook.get_conn()
    cursor = conn.cursor()
    
    create_table_query = """
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
    cursor.execute(create_table_query)
    conn.commit()

    create_stage_query = """
    CREATE STAGE IF NOT EXISTS my_stage;
    """
    cursor.execute(create_stage_query)
    conn.commit()

    with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmp_file:
        df.to_csv(tmp_file.name, index=False, header=True)
        tmp_file_path = tmp_file.name

    put_query = f"PUT file://{tmp_file_path} @my_stage"
    cursor.execute(put_query)
    conn.commit()
    
    copy_query = """
    COPY INTO MASON.FINANCIAL_ENTITY_ANNUAL_TIME_SERIES_TRANSFORMED
    FROM @my_stage
    FILE_FORMAT = (TYPE = CSV, FIELD_OPTIONALLY_ENCLOSED_BY = '"', SKIP_HEADER = 1)
    """
    cursor.execute(copy_query)
    conn.commit()

    logging.info(f"Data loaded to Snowflake table MASON.FINANCIAL_ENTITY_ANNUAL_TIME_SERIES_TRANSFORMED")
    os.remove(tmp_file_path) 

extract_task = PythonOperator(
    task_id='extract_from_oracle',
    python_callable=extract_from_oracle,
    op_kwargs={'file_path': '/tmp/extracted_data.json'},
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    op_kwargs={
        'input_path': '/tmp/extracted_data.json',
        'output_path': '/tmp/transformed_data.json'
    },
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_snowflake',
    python_callable=load_to_snowflake,
    op_kwargs={'file_path': '/tmp/transformed_data.json'},
    dag=dag,
)

extract_task >> transform_task >> load_task