import os, glob, airflow, boto3

from datetime import datetime, timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.models import Variable

# Operators; we need this to operate!
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.operators.athena import AWSAthenaOperator
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator
from airflow.providers.amazon.aws.operators.s3_copy_object import S3CopyObjectOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.transfers.s3_to_gcs import S3ToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from s3_to_dwh.lib.config_loader import ConfigLoader
from s3_to_dwh.lib.query_builder import QueryBuilder

with DAG(
    's3_to_dwh',
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
    description='s3 to dwh DAG',
    schedule_interval='0 0 1 * *',
    start_date=datetime(2022, 4, 1),
    catchup=False,
    tags=['sre'],
) as dag:

    @dag.task(task_id='data_interval_date')
    def data_interval_date(start: str, end: str):
        data_interval_start = datetime.fromisoformat(start)
        data_interval_end = datetime.fromisoformat(end)
        return {
            'data_interval_start': str(data_interval_start.date()),
            'data_interval_end': str(data_interval_end.date() - timedelta(1)),
        }

    data_interval_date = data_interval_date(
        start="{{ data_interval_start }}",
        end="{{ data_interval_end }}",
    )

    variable = Variable.get('s3_to_dwh', deserialize_json=True)
    for yml in glob.glob('{dags_folder}/s3_to_dwh/config/**/monthly/*.yml'.format(dags_folder=airflow.settings.DAGS_FOLDER), recursive=True):
        config = ConfigLoader(yml).load()
        queryBuilder = QueryBuilder(config)

        table_name = config['table']['name']
        table_origin_name = config['table']['origin']
        prefix = config['table']['prefix']
        output = variable['s3']['output']

        partition = config['table']['partition']
        # TODO: WRITE_TRUNCATE and WRITE_APPEND switching implementation
        mode = config['table']['mode']

        @dag.task(task_id='ready_{table_name}'.format(table_name=table_name))
        def ready(source_bucket_name: str, source_bucket_key: str, dest_bucket_name: str, dest_bucket_key: str):
            session = AwsBaseHook(aws_conn_id='aws_default', resource_type='s3').get_session()
            for obj in session.resource('s3').Bucket(source_bucket_name).objects.filter(Prefix=source_bucket_key):
                if 0 < len(os.path.basename(obj.key)):
                    print('s3://{bucket}/{key}'.format(bucket=source_bucket_name, key=obj.key))
                    copy_source = { 'Bucket': source_bucket_name, 'Key': obj.key}
                    session.resource('s3').meta.client.copy(copy_source, dest_bucket_name, '{dest_bucket_key}{basename}'.format(dest_bucket_key=dest_bucket_key, basename=os.path.basename(obj.key)))

        ready_raw = ready(
            source_bucket_name=variable['s3']['source'],
            source_bucket_key='{prefix}{dt}'.format(prefix=prefix, dt="{{ ti.xcom_pull(task_ids='data_interval_date')['data_interval_end'] }}"),
            dest_bucket_name=variable['s3']['raw'],
            dest_bucket_key='{prefix}dt={dt}/'.format(prefix=prefix, dt="{{ ti.xcom_pull(task_ids='data_interval_date')['data_interval_end'] }}"),
        )

        # --------------------------------------------------------------
        # RAW
        # --------------------------------------------------------------

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
            query=queryBuilder.create_table(raw_location),
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

        # --------------------------------------------------------------
        # INTERMEDIATE
        # --------------------------------------------------------------

        ready_intermediate = S3DeleteObjectsOperator(
            task_id='ready_intermediate_{table_name}'.format(table_name=table_name),
            bucket=variable['s3']['intermediate'],
            prefix=prefix
        )

        drop_intermediate = AWSAthenaOperator(
            task_id='drop_intermediate_{table_name}'.format(table_name=table_name),
            query=queryBuilder.drop_table(),
            database=queryBuilder.db_name('i'),
            output_location='s3://{output}/{prefix}'.format(output=output, prefix=prefix),
            sleep_time=30,
            max_tries=None,
        )

        intermediate_location = 's3://{bucket}/{prefix}'.format(bucket=variable['s3']['intermediate'], prefix=prefix)
        create_intermediate = AWSAthenaOperator(
            task_id='create_intermediate_{table_name}'.format(table_name=table_name),
            query=queryBuilder.ctas_parquet(
                source=queryBuilder.db_name('r'),
                partitions=partition,
                dt="{{ ti.xcom_pull(task_ids='data_interval_date')['data_interval_end'] }}",
                prefix=intermediate_location
            ),
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

        # --------------------------------------------------------------
        # STRUCTURALIZATION
        # --------------------------------------------------------------


        # --------------------------------------------------------------
        # FINALIZER for AWS
        # --------------------------------------------------------------
        ready_finalize = S3DeleteObjectsOperator(
            task_id='ready_finalize_{table_name}'.format(table_name=table_name),
            bucket=variable['s3']['finalize'],
            prefix=prefix
        )

        drop_finalize = AWSAthenaOperator(
            task_id='drop_finalize_{table_name}'.format(table_name=table_name),
            query=queryBuilder.drop_table(),
            database=queryBuilder.db_name('f'),
            output_location='s3://{output}/{prefix}'.format(output=output, prefix=prefix),
            sleep_time=30,
            max_tries=None,
        )

        finalize_location = 's3://{bucket}/{prefix}'.format(bucket=variable['s3']['finalize'], prefix=prefix)
        create_finalize = AWSAthenaOperator(
            task_id='create_finalize_{table_name}'.format(table_name=table_name),
            query=queryBuilder.ctas(
                source=queryBuilder.db_name('i'),
                location='{location}{dt}/'.format(location=finalize_location, dt="{{ ti.xcom_pull(task_ids='data_interval_date')['data_interval_end'] }}")
            ),
            database=queryBuilder.db_name('f'),
            output_location='s3://{output}/{prefix}'.format(output=output, prefix=prefix),
            sleep_time=30,
            max_tries=None,
        )

        # --------------------------------------------------------------
        #
        # --------------------------------------------------------------
        ready_gcs = GCSDeleteObjectsOperator(
            task_id='ready_gcs_{table_name}'.format(table_name=table_name),
            bucket_name=variable['gcs']['destination'],
            prefix='{prefix}{dt}/'.format(prefix=prefix, dt="{{ ti.xcom_pull(task_ids='data_interval_date')['data_interval_end'] }}"),
        )

        s3_to_gcs = S3ToGCSOperator(
            task_id='s3_to_gcs_{table_name}'.format(table_name=table_name),
            bucket=variable['s3']['finalize'],
            prefix='{prefix}{dt}/'.format(prefix=prefix, dt="{{ ti.xcom_pull(task_ids='data_interval_date')['data_interval_end'] }}"),
            dest_gcs='gs://{gcs_bucket}/'.format(gcs_bucket=variable['gcs']['destination']),
            replace=True,
            gzip=False,
        )

        gcs_to_bq = GCSToBigQueryOperator(
            task_id='gcs_to_bq_{table_name}'.format(table_name=table_name),
            bucket='{gcs_bucket}'.format(gcs_bucket=variable['gcs']['destination']),
            source_objects=['{prefix}{dt}/*'.format(prefix=prefix, dt="{{ ti.xcom_pull(task_ids='data_interval_date')['data_interval_end'] }}"),],
            source_format='PARQUET',
            compression='GZIP',
            destination_project_dataset_table='{dataset}.{table_name}'.format(dataset=variable['bigquery']['dataset'], table_name=table_origin_name),
            write_disposition=mode,
        )

        (
            data_interval_date
            >> ready_raw >> drop_raw >> create_raw >> msk_repaire_raw
            >> ready_intermediate >> drop_intermediate >> create_intermediate >> msk_repaire_intermediate
            >> ready_finalize >> drop_finalize >> create_finalize
            >> ready_gcs >> s3_to_gcs >> gcs_to_bq
        )
