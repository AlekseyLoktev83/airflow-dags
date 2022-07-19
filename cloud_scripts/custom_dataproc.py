from typing import Sequence
from airflow.providers.yandex.hooks.yandexcloud_dataproc import DataprocCreatePysparkJobOperator

class DataprocCreatePysparkJobOperator(DataprocCreatePysparkJobOperator):
    template_fields: Sequence[str] = ('cluster_id','args',)
