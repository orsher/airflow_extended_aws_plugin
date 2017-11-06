from airflow import DAG
from airflow.models import Variable
from airflow.operators import ExtendedEmrCreateJobFlowOperator, LivySparkOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from datetime import datetime, timedelta, time

DAG_NAME = 'daily_spark_job'
AWS_BUCKET_VAR_KEY = "aws_bucket"
EC2_KEY_NAME_VAR_KEY = "ec2_key_name"
API_PARAMS_OVERRIDE_VAR_KEY = "api_params_override"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.combine(datetime.today() - timedelta(days=1), time(2, 0)),
    'email_on_failure': True,
    'email_on_retry': True,
}

dag = DAG(
    DAG_NAME, default_args=default_args, schedule_interval='@daily')

# Specifying default params for api
api_params = {'Name': 'daily_spark_job',
              'LogUri': 's3://' + Variable.get(DAG_NAME + "." + AWS_BUCKET_VAR_KEY) + '/emr/logs',
              'ReleaseLabel': 'emr-5.9.0',
              'Instances': {
                  "InstanceGroups": [{
                      "Name": "Master nodes",
                      "Market": "ON_DEMAND",
                      "InstanceRole": "MASTER",
                      "InstanceType": "m3.xlarge",
                      "InstanceCount": 1
                  }],
                  'Ec2KeyName': Variable.get(DAG_NAME + "." + EC2_KEY_NAME_VAR_KEY),
                  'KeepJobFlowAliveWhenNoSteps': True,
                  'TerminationProtected': False,
              },
              "Applications": [{
                  "Name": "Spark"
              }, {
                  "Name": "Livy"
              }],

              "VisibleToAllUsers": True,
              "JobFlowRole": "EMR_EC2_DefaultRole",
              "ServiceRole": "EMR_DefaultRole",
              "Tags": []
              }
# Update api params using configurable variable
api_params.update(Variable.get(DAG_NAME + "." + API_PARAMS_OVERRIDE_VAR_KEY, deserialize_json=True, default_var={}))

# Create a cluster and wait until it get to status 'Waiting' and then save a new connection to the Livy service
create_cluster = ExtendedEmrCreateJobFlowOperator(
    task_id='create_cluster',
    aws_conn_id='aws_default',
    api_params=api_params,
    wait_for_status='WAITING',
    save_livy_connection_name='Daily-Livy-Spark',
    dag=dag
)

# Run the LiveySparkOperator ( https://github.com/rssanders3/airflow-spark-operator-plugin )
spark_job = LivySparkOperator(
    task_id='spark_job',
    spark_script='example_pyspark_script.py',
    http_conn_id='Daily-Livy-Spark',
    session_kind='pyspark',
    dag=dag)

# Terminate the cluster using the built in operator
terminate_cluster = EmrTerminateJobFlowOperator(
    task_id='terminate_cluster',
    job_flow_id="{{ task_instance.xcom_pull('create_cluster') }}",
    aws_conn_id='aws_default',
    dag=dag
)

# Configure dependencies
create_cluster >> spark_job
spark_job >> terminate_cluster
