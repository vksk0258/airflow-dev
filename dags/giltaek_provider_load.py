from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
import common.functions as fn

with DAG(
    dag_id="giltaek_provider_load",
    # 이덱은 매일 6시 30분에 시작
    schedule="30 6 * * *",
    # 덱이 언제 부터 돌지 TZ=타임존
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"),
    # 캐치업 배치 중간에 누락된 구간을 돌릴지 말지
    # 1월 1일부터 3월 1일까지 누락된 덱을 한번에 돌아가게 된다 3월 1일에 왠만하면 false
    catchup=False,
    tags=["oracle", "snowflake", "pandas"]
) as dag:
    extract_from_oracle = PythonOperator(
        task_id='extract_from_oracle_all',
        python_callable=fn.extract_from_oracle
    )

    load_data = PythonOperator(
        task_id='load_data',
        python_callable=fn.transform_data
    )

    extract_from_oracle >> load_data