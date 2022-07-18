from typing import TYPE_CHECKING, Dict, Iterable, Optional, Sequence, Union

from airflow.models import BaseOperator
from airflow.providers.yandex.hooks.yandexcloud_dataproc import DataprocHook
import json

if TYPE_CHECKING:
    from airflow.utils.context import Context

class DataprocCreatePysparkJobOperator(BaseOperator):
    """Runs Pyspark job in Data Proc cluster.

    :param main_python_file_uri: URI of python file with job. Can be placed in HDFS or S3.
    :param python_file_uris: URIs of python files used in the job. Can be placed in HDFS or S3.
    :param file_uris: URIs of files used in the job. Can be placed in HDFS or S3.
    :param archive_uris: URIs of archive files used in the job. Can be placed in HDFS or S3.
    :param jar_file_uris: URIs of JAR files used in the job. Can be placed in HDFS or S3.
    :param properties: Properties for the job.
    :param args: Arguments to be passed to the job.
    :param name: Name of the job. Used for labeling.
    :param cluster_id: ID of the cluster to run job in.
                       Will try to take the ID from Dataproc Hook object if it's specified. (templated)
    :param connection_id: ID of the Yandex.Cloud Airflow connection.
    :param packages: List of maven coordinates of jars to include on the driver and executor classpaths.
    :param repositories: List of additional remote repositories to search for the maven coordinates
                         given with --packages.
    :param exclude_packages: List of groupId:artifactId, to exclude while resolving the dependencies
                         provided in --packages to avoid dependency conflicts.
    """

    template_fields: Sequence[str] = ('cluster_id','args',)

    def __init__(
        self,
        *,
        main_python_file_uri: Optional[str] = None,
        python_file_uris: Optional[Iterable[str]] = None,
        jar_file_uris: Optional[Iterable[str]] = None,
        archive_uris: Optional[Iterable[str]] = None,
        file_uris: Optional[Iterable[str]] = None,
        args: Optional[Iterable[str]] = None,
        properties: Optional[Dict[str, str]] = None,
        name: str = 'Pyspark job',
        cluster_id: Optional[str] = None,
        connection_id: Optional[str] = None,
        packages: Optional[Iterable[str]] = None,
        repositories: Optional[Iterable[str]] = None,
        exclude_packages: Optional[Iterable[str]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.main_python_file_uri = main_python_file_uri
        self.python_file_uris = python_file_uris
        self.jar_file_uris = jar_file_uris
        self.archive_uris = archive_uris
        self.file_uris = file_uris
        self.args = args
        self.properties = properties
        self.name = name
        self.cluster_id = cluster_id
        self.connection_id = connection_id
        self.packages = packages
        self.repositories = repositories
        self.exclude_packages = exclude_packages
        self.hook: Optional[DataprocHook] = None

    def execute(self, context: 'Context') -> None:
        cluster_id = self.cluster_id or context['task_instance'].xcom_pull(key='cluster_id')
#         args = self.args or json.loads(context['task_instance'].xcom_pull(key='args'))
       
        yandex_conn_id = self.connection_id or context['task_instance'].xcom_pull(
            key='yandexcloud_connection_id'
        )
        self.hook = DataprocHook(
            yandex_conn_id=yandex_conn_id,
        )
        self.hook.client.create_pyspark_job(
            main_python_file_uri=self.main_python_file_uri,
            python_file_uris=self.python_file_uris,
            jar_file_uris=self.jar_file_uris,
            archive_uris=self.archive_uris,
            file_uris=self.file_uris,
            args=self.args,
            properties=self.properties,
            packages=self.packages,
            repositories=self.repositories,
            exclude_packages=self.exclude_packages,
            name=self.name,
            cluster_id=cluster_id,
        )
