from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadAWSOperator(BaseOperator):

    @apply_defaults
    def __init__(self, bucket, access_key, secret_key, session_token, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bucket = bucket
        self.access_key = access_key
        self.secret_key = secret_key
        self.session_token = session_token

    def execute(self, context):
        from ibov_techchallenge_dois.scripts.load_aws import handle_s3
        file_path, date = context['task_instance'].xcom_pull(task_ids='transform_task')
        prefix = date
        return handle_s3(file_path, self.bucket, self.access_key, self.secret_key, self.session_token, prefix)
