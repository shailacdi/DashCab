"""
This is a DAG for airflow. This is configured to run the following tasks using a
sequential executor
1. upload files to S3
2. schema filter (initiates batch processing of individual past trip files)
3. process stats (derives metrics from historical trip data)
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime,timedelta

WORKING_DIR = '/home/ubuntu/DashCab/'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 10, 1),
    'email': ['shailacdi@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes= 5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('dashcab', default_args=default_args, schedule_interval=timedelta(days=30))


t1 = BashOperator(
        task_id='upload_data_to_s3',
        bash_command='cd /home/ubuntu/DashCab/; ./upload_dataset_to_s3.sh ',
        dag=dag)

t2 = BashOperator(
        task_id='start_batch_job',
        bash_command='cd /home/ubuntu/DashCab/; ./start_batch_job.sh ', #a space is necessary after .sh
        dag=dag)
t2.set_upstream(t1)

t3 = BashOperator(
    task_id='process_trip_stats',
    bash_command='cd /home/ubuntu/DashCab/; ./process_stats.sh ',
    dag=dag)
t3.set_upstream(t2)
