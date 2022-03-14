import os, glob

from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.models import Variable

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.athena import AWSAthenaOperator
from athena_to_bigquery.lib.config_loader import ConfigLoader
from athena_to_bigquery.lib.query_builder import QueryBuilder

with DAG(
    'athena_to_bigquery',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email': ['panicboat+airflow@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function,
        # 'on_success_callback': some_other_function,
        # 'on_retry_callback': another_function,
        # 'sla_miss_callback': yet_another_function,
        # 'trigger_rule': 'all_success'
    },
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['sandbox'],
) as dag:

    variable = Variable.get('athena_to_bigquery', deserialize_json=True)
    for yml in glob.glob('./dags/athena_to_bigquery/config/*'):
        config = ConfigLoader(yml).load()
        create_table_sql = QueryBuilder(config).create_table(variable['s3']['source'])
        print(create_table_sql)

        create_table = AWSAthenaOperator(
            task_id=f'create_{config['table']['name']}',
            query=create_table_sql,
            database='data_lake_raw',
            output_location=variable['s3']['output'],
            sleep_time=30,
            max_tries=None,
        )
