import os, glob, boto3

from datetime import datetime, timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.models import Variable

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.operators.athena import AWSAthenaOperator
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator
from airflow.providers.amazon.aws.operators.s3_copy_object import S3CopyObjectOperator

from athena_to_bigquery.lib.config_loader import ConfigLoader
from athena_to_bigquery.lib.query_builder import QueryBuilder

with DAG(
    'athena_to_bigquery',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': True,
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

    dt = '{dt}'.format(dt="{{ ds }}")
    variable = Variable.get('athena_to_bigquery', deserialize_json=True)
    for yml in glob.glob('./dags/athena_to_bigquery/config/**/*.yml', recursive=True):
        config = ConfigLoader(yml).load()
        queryBuilder = QueryBuilder(config)

        table_name = config['table']['name']
        prefix = config['table']['prefix']
        output = variable['s3']['output']

        partition = config['table']['partition']
        # TODO: REPLACE and ADD switching implementation
        mode = config['table']['mode']

        @dag.task(task_id='ready_{table_name}'.format(table_name=table_name))
        def ready(source_bucket_name: str, source_bucket_key: str, dest_bucket_name: str, dest_bucket_key: str):
            session = AwsBaseHook(aws_conn_id='aws_default', resource_type='s3').get_session()
            for obj in session.resource('s3').Bucket(source_bucket_name).objects.filter(Prefix=source_bucket_key):
                if 0 < len(os.path.basename(obj.key)):
                    copy_source = { 'Bucket': source_bucket_name, 'Key': obj.key}
                    session.resource('s3').meta.client.copy(copy_source, dest_bucket_name, '{dest_bucket_key}{basename}'.format(dest_bucket_key=dest_bucket_key, basename=os.path.basename(obj.key)))

        copy_to_raw = ready(
            source_bucket_name=variable['s3']['source'],
            source_bucket_key='{prefix}{dt}'.format(prefix=prefix, dt=dt),
            dest_bucket_name=variable['s3']['raw'],
            dest_bucket_key='{prefix}dt={dt}/'.format(prefix=prefix, dt=dt),
        )

        drop_raw = AWSAthenaOperator(
            task_id='drop_raw_{table_name}'.format(table_name=table_name),
            query=queryBuilder.drop_table(),
            database=queryBuilder.db_name('r'),
            output_location='s3://{output}/{prefix}'.format(output=output, prefix=prefix),
            sleep_time=30,
            max_tries=None,
        )

        raw_location = 's3://{bucket}/{prefix}'.format(bucket=variable['s3']['raw'], prefix=prefix)
        create_raw = AWSAthenaOperator(
            task_id='create_raw_{table_name}'.format(table_name=table_name),
            query=queryBuilder.create_table_raw(raw_location),
            database=queryBuilder.db_name('r'),
            output_location='s3://{output}/{prefix}'.format(output=output, prefix=prefix),
            sleep_time=30,
            max_tries=None,
        )

        msk_repaire_raw = AWSAthenaOperator(
            task_id='msk_repaire_raw_{table_name}'.format(table_name=table_name),
            query='MSCK REPAIR TABLE {table_name}'.format(table_name=table_name),
            database=queryBuilder.db_name('r'),
            output_location='s3://{output}/{prefix}'.format(output=output, prefix=prefix),
            sleep_time=30,
            max_tries=None,
        )

        drop_intermediate = AWSAthenaOperator(
            task_id='drop_intermediate_{table_name}'.format(table_name=table_name),
            query=queryBuilder.drop_table(),
            database=queryBuilder.db_name('i'),
            output_location='s3://{output}/{prefix}'.format(output=output, prefix=prefix),
            sleep_time=30,
            max_tries=None,
        )

        delete_intermediate = S3DeleteObjectsOperator(
            task_id='delete_intermediate_{table_name}'.format(table_name=table_name),
            bucket=variable['s3']['intermediate'],
            prefix=prefix
        )

        intermediate_location = 's3://{bucket}/{prefix}'.format(bucket=variable['s3']['intermediate'], prefix=prefix)
        create_intermediate = AWSAthenaOperator(
            task_id='create_intermediate_{table_name}'.format(table_name=table_name),
            query=queryBuilder.create_table_intermediate(partition, dt, intermediate_location),
            database=queryBuilder.db_name('i'),
            output_location='s3://{output}/{prefix}'.format(output=output, prefix=prefix),
            sleep_time=30,
            max_tries=None,
        )

        msk_repaire_intermediate = AWSAthenaOperator(
            task_id='msk_repaire__{table_name}'.format(table_name=table_name),
            query='MSCK REPAIR TABLE {table_name}'.format(table_name=table_name),
            database=queryBuilder.db_name('i'),
            output_location='s3://{output}/{prefix}'.format(output=output, prefix=prefix),
            sleep_time=30,
            max_tries=None,
        )

        copy_to_raw >> drop_raw >> create_raw >> msk_repaire_raw >> drop_intermediate >> delete_intermediate >> create_intermediate >> msk_repaire_intermediate
