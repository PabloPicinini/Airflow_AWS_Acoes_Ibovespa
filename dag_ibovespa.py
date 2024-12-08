import os
from airflow.models import DAG
from datetime import timedelta
import pendulum
from ibov_techchallenge_dois.operators.download_acao_operator import DownloadAcaoOperator
from ibov_techchallenge_dois.operators.transform_operator import TransformOperator
from ibov_techchallenge_dois.operators.load_aws_operator import LoadAWSOperator
from dotenv import load_dotenv

load_dotenv()

# Pegue as variáveis de ambiente
BUCKET = os.getenv('BUCKET')
ACCESS_KEY = os.getenv('ACCESS_KEY')
SECRET_KEY = os.getenv('SECRET_KEY')
SESSION_TOKEN = os.getenv('SESSION_TOKEN')

def_dag = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.now('UTC').subtract(days=1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='dag_acao_ibovespa',
    default_args=def_dag,
    schedule_interval='@daily'
) as dag:

    download_task = DownloadAcaoOperator(task_id='download_task')
    transform_task = TransformOperator(task_id='transform_task')
    load_task = LoadAWSOperator(
        task_id='load_task',
        bucket=BUCKET,
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        session_token=SESSION_TOKEN
    )

    download_task >> transform_task >> load_task
