# kafka_example_dag_1.py 

import os
import json
import logging
import functools
from pendulum import datetime

from airflow import DAG
from airflow_provider_kafka.operators.produce_to_topic import ProduceToTopicOperator

# get the topic name from .env
my_topic = "jupiter"

# get Kafka configuration information
connection_config = {
    "bootstrap.servers": "rc1b-0qd2fn83sq9vp78r.mdb.yandexcloud.net:9092",
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "SCRAM-SHA-512",
    "sasl.username": "jupiter-user",
    "sasl.password": "pass1234",
    "ssl.ca.location": "/opt/ssl2/ca-cert"
}

with DAG(
    dag_id="kafka_test",
    start_date=datetime(2022, 11, 1),
    schedule=None,
    catchup=False,
):

    # define the producer function
    def producer_function():
        for i in range(5):
            yield (json.dumps(i), json.dumps(i+1))

    # define the producer task
    producer_task = ProduceToTopicOperator(
        task_id=f"produce_to_{my_topic}",
        topic=my_topic,
        producer_function=producer_function, 
        kafka_config=connection_config
    )
