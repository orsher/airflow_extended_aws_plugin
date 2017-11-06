# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from airflow import models
from airflow import settings
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.exceptions import AirflowException
import time
import logging


class ExtendedEmrCreateJobFlowOperator(BaseOperator):
    TERMINATE_STATES = ['TERMINATING', 'TERMINATED', 'TERMINATED_WITH_ERRORS']

    @apply_defaults
    def __init__(self,
                 aws_conn_id='s3_default',
                 api_params=None,
                 wait_for_status=None,
                 save_livy_connection_name=None, **kwargs):
        """

        :param aws_conn_id: Airflow connection id specifying credentials to AWS account
        :param api_params: Api parameters for the boto create job flow call
        :param wait_for_status: When supplied, the operator will wait until the cluster will get to the supplied status
        and fail if it get to a terminating status before
        :param save_livy_connection_name: When supplied, the operator will save a new airflow connection with the
        supplied name with the master node public ip and the Livy default port. The connection could be later on be used
        by LivySparkOperator from the following plugin:  https://github.com/rssanders3/airflow-spark-operator-plugin
        """
        super(ExtendedEmrCreateJobFlowOperator, self).__init__(**kwargs)
        self.aws_conn_id = aws_conn_id
        self.api_params = api_params
        self.wait_for_status = wait_for_status
        self.save_livy_connection_name = save_livy_connection_name

    def execute(self, context):
        logging.info("Executing ExtendedEmrCreateJobFlowOperator")
        aws = AwsHook(aws_conn_id=self.aws_conn_id)
        emr = aws.get_client_type('emr')

        response = emr.run_job_flow(
            Name=self.api_params.get('Name'),
            LogUri=self.api_params.get('LogUri'),
            ReleaseLabel=self.api_params.get('ReleaseLabel'),
            Instances=self.api_params.get('Instances'),
            Steps=self.api_params.get('Steps', []),
            BootstrapActions=self.api_params.get('BootstrapActions', []),
            Applications=self.api_params.get('Applications'),
            Configurations=self.api_params.get('Configurations', []),
            VisibleToAllUsers=self.api_params.get('VisibleToAllUsers'),
            JobFlowRole=self.api_params.get('JobFlowRole'),
            ServiceRole=self.api_params.get('ServiceRole'),
            Tags=self.api_params.get('Tags'),
        )
        if not response['ResponseMetadata']['HTTPStatusCode'] == 200:
            raise AirflowException('JobFlow creation failed: %s' % response)
        else:
            logging.info('JobFlow with id %s created', response['JobFlowId'])
            job_flow_id = response['JobFlowId']

        if self.wait_for_status is not None:
            status = emr.describe_cluster(ClusterId=job_flow_id)['Cluster']['Status']['State']
            while status != self.wait_for_status and status not in self.TERMINATE_STATES:
                logging.info("Waiting for status %s. Current status is %s", self.wait_for_status, status)
                time.sleep(30)
                status = emr.describe_cluster(ClusterId=job_flow_id)['Cluster']['Status']['State']
            if status in self.TERMINATE_STATES:
                raise AirflowException('Cluster was terminated [%s] before it got to status %s' %
                                       (status, self.wait_for_status))

        if self.save_livy_connection_name is not None:
            instances_response = emr.list_instances(ClusterId=job_flow_id, InstanceGroupTypes=['MASTER'])
            master_ip = instances_response['Instances'][0]['PublicIpAddress']
            ExtendedEmrCreateJobFlowOperator.create_or_replace_connection(connection_id=self.save_livy_connection_name,
                                                                          connection_type='Livy',
                                                                          ip="http://" + master_ip, port=8998, login='',
                                                                          password='', schema='', extra='')

        return job_flow_id

    @staticmethod
    def delete_connection(connection_id):
        """

        :param connection_id: Airflow connection_id to be deleted
        """
        session = settings.Session()
        C = models.Connection
        session.query(C).filter(C.conn_id == connection_id).delete()

    @staticmethod
    def create_or_replace_connection(connection_id, connection_type, ip, port, login, password, schema, extra):
        """

        :param connection_id: Airflow connection_id to be created or replaced
        :param connection_type: Airflow connection type
        :param ip: Airflow connection ip
        :param port: Airflow connection port
        :param login: Airflow connection login/user
        :param password: Airflow connection password
        :param schema: Airflow connection schema
        :param extra: Airflow connection extra parameter supplied as json string
        """
        session = settings.Session()
        conn = models.Connection(
            conn_id=connection_id, conn_type=connection_type,
            host=ip, port=port, login=login, password=password,
            schema=schema,
            extra=extra)
        ExtendedEmrCreateJobFlowOperator.delete_connection(connection_id)
        session.add(conn)
        session.commit()


# Defining the plugin class
class ExtendedAWSPlugin(AirflowPlugin):
    name = "extended_aws_plugin"
    operators = [ExtendedEmrCreateJobFlowOperator]
