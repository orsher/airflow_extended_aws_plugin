# Airflow Extended AWS Plugin
An Airflow plugin which supply some AWS operators with extended feature over the build in Airflow's operators

# Deployment
1. Copy the extended_aws_plugin.py into you Airflow's plugins directory.
2. Create A DAG using the operators ( take a look at the examples ) and put it under the DAGS directory
3. Configure the aws_default connection
3. Restart Airflow services.

# Operators

## ExtendedEmrCreateJobFlowOperator

The ExtendedEmrCreateJobFlowOperator uses the built in aws_hook and give the following enhancements over the built in EmrCreateJobFlowOperator:
1. Can optionally keep the cluster up and running even if you submit the create job flow without any steps or want the cluster to keep running even after it finished all steps.
2. Can create an Airflow connection to the created Livy service. This can later on be used by LivySparkOperator to submit concurrent spark jobs to the cluster while keeping contact with the running jobs. Check out the airflow spark plugin supplying the ability to run jobs using Livy: https://github.com/rssanders3/airflow-spark-operator-plugin
3. Specify the default api params inside the operator definition and not on an "emr_connection".

