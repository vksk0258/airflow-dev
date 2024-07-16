# 현대홈쇼핑 airflow 개발 repo

## - 필수사항
  - **무조건 git push 전 git patch => git pull 코드 최신화 (브랜치 충돌 방지)**
  - **yaml 파일 및 airflow.cfg 파일 변경 및 서버 환경 변경 사항은 무조건 기입**
  - **자신이 만든 덱 .py는 자신의 이름 앞에 기입  >> 이름_덱이름.py <<**
      - ex) giltaek_oracle_to_snowflake.py 

## - 덱 정의
  - 나문오
    - moonoh_echo_test : Airflow DAG 테스트용 echo 테스트
    - moonoh_oracle_etl_test_dag : Oracle to Oracle 프로세스 Sort 사용
    - moonoh_snowflake_transform_test : Oracle to Snowflake 프로세스 트랜스폼 사용
  - 김예진
  - 나성천
  - 유길택
    - giltaek_oracle_to_snowflake.py : oracle to snowflake 프로세스 pandas pivot으로 트랜스폼 사용

## - 특이사항 이력
  - (2024.07.09) : git 최초 배포 및 yaml파일로 dag path 수정
    - airflow/dag => airflow/airflow-dev/dag
  - (2024.07.09) : airflow.cfg 로그 타입 INFO => DEBUG 파라미터 변경
  - (2024.07.15)dkpoc05 airflow 서버 새로 설치
    - version
      - airflow 2.7.3
      - python 3.9
      - requirements.txt
    - 이미지 생성 : airflow273_py39
## - git 작성자
  - 나문오
  - 김예진
  - 나성천
  - 유길택

