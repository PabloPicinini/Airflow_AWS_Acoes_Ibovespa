from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DownloadAcaoOperator(BaseOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def execute(self, context):
        from ibov_techchallenge_dois.scripts.extract import download_pregao
        download_pregao()
