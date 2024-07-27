# Import necessary modules from Airflow and other libraries
from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.dummy_operator import DummyOperator
import boto3
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator, 
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator)
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor, EmrStepSensor

# Define configuration for the EMR cluster
job_flow_overrides = {
    "Name": "redfin_emr_cluster",
    "ReleaseLabel": "emr-6.13.0",  # Specifies the EMR version to use
    "Applications": [{"Name": "Spark"}, {"Name": "JupyterEnterpriseGateway"}],  # Applications to install
    "LogUri": "s3://redfin-data-project-ak/emr-logs-yml/",  # S3 location for EMR logs
    "VisibleToAllUsers": False,
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core node",
                "Market": "SPOT",  # Use spot instances for cost-saving
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2,
            },
        ],
        "Ec2SubnetId": "subnet-0e2cc431d495d81a1",  # Subnet ID for EMR cluster
        "Ec2KeyName": 'emr-keypair-airflow',  # EC2 key pair for SSH access
        "KeepJobFlowAliveWhenNoSteps": True,  # Keep cluster alive even if no steps are running
        "TerminationProtected": False,  # Allow programmatic termination of the cluster
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",  # Default EMR EC2 role
    "ServiceRole": "EMR_DefaultRole",  # Default EMR service role
}

# Define the Spark steps for data extraction
SPARK_STEPS_EXTRACTION = [
    {
        "Name": "Extract Redfin data",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar",
            "Args": [
                "s3://redfin-data-project-ak/scripts/ingest.sh",
            ],
        },
    },
]

# Define the Spark steps for data transformation
SPARK_STEPS_TRANSFORMATION = [
    {
        "Name": "Transform Redfin data",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": ["spark-submit",
            "s3://redfin-data-project-ak/scripts/transform_redfin_data.py",
            ],
        },
    },
]

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 23),
    'email': ['myemail@domain.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=10)
}

# Define the DAG and its tasks
with DAG('redfin_analytics_spark_job_dag',
         default_args=default_args,
         #schedule_interval = '@weekly',
         catchup=False) as dag:

    # Dummy task to mark the start of the pipeline
    start_pipeline = DummyOperator(task_id="tsk_start_pipeline")

    # Task to create an EMR cluster
    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="tsk_create_emr_cluster",
        job_flow_overrides=job_flow_overrides,
        #aws_conn_id="aws_default",
        #emr_conn_id="emr_default",
    )

    # Sensor to monitor the creation of the EMR cluster
    is_emr_cluster_created = EmrJobFlowSensor(
        task_id="tsk_is_emr_cluster_created", 
        job_flow_id="{{ task_instance.xcom_pull(task_ids='tsk_create_emr_cluster', key='return_value') }}",
        target_states={"WAITING"},  # Desired state for the cluster to reach
        timeout=3600,
        poke_interval=5,
        mode='poke',
    )

    # Task to add data extraction steps to the EMR cluster
    add_extraction_step = EmrAddStepsOperator(
        task_id="tsk_add_extraction_step",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='tsk_create_emr_cluster', key='return_value') }}",
        # aws_conn_id="aws_default",
        steps=SPARK_STEPS_EXTRACTION,
        # do_xcom_push=True, # Enable XCom push to monitor step status
    )

    # Sensor to monitor the completion of the extraction step
    is_extraction_completed = EmrStepSensor(
        task_id="tsk_is_extraction_completed",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='tsk_create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='tsk_add_extraction_step')[0] }}",
        target_states={"COMPLETED"},
        timeout=3600,
        poke_interval=5,
    )

    # Task to add data transformation steps to the EMR cluster
    add_transformation_step = EmrAddStepsOperator(
        task_id="tsk_add_transformation_step",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='tsk_create_emr_cluster', key='return_value') }}",
        # aws_conn_id="aws_default",
        steps=SPARK_STEPS_TRANSFORMATION,
        # do_xcom_push=True, # Enable XCom push to monitor step status
    )

    # Sensor to monitor the completion of the transformation step
    is_transformation_completed = EmrStepSensor(
        task_id="tsk_is_transformation_completed",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='tsk_create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='tsk_add_transformation_step')[0] }}",
        target_states={"COMPLETED"},
        timeout=3600,
        poke_interval=10,
    )

    # Task to terminate the EMR cluster
    remove_cluster = EmrTerminateJobFlowOperator(
        task_id="tsk_remove_cluster",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='tsk_create_emr_cluster', key='return_value') }}",
    )

    # Sensor to monitor the termination of the EMR cluster
    is_emr_cluster_terminated = EmrJobFlowSensor(
        task_id="tsk_is_emr_cluster_terminated", 
        job_flow_id="{{ task_instance.xcom_pull(task_ids='tsk_create_emr_cluster', key='return_value') }}",
        target_states={"TERMINATED"},  # Desired state for the cluster to reach
        timeout=3600,
        poke_interval=5,
        mode='poke',
    )

    # Dummy task to mark the end of the pipeline
    end_pipeline = DummyOperator(task_id="tsk_end_pipeline")

    # Define the task dependencies and workflow sequence
    start_pipeline >> create_emr_cluster >> is_emr_cluster_created >> add_extraction_step >> is_extraction_completed >> add_transformation_step >> is_transformation_completed >> remove_cluster
    remove_cluster >> is_emr_cluster_terminated >> end_pipeline
