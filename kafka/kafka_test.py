# kafka_example_dag_1.py 

import os
import json
import logging
import functools
from pendulum import datetime

from airflow import DAG
from airflow_provider_kafka.operators.produce_to_topic import ProduceToTopicOperator
from airflow_provider_kafka.operators.consume_from_topic import ConsumeFromTopicOperator

# get the topic name from .env
my_topic = "jupiter"
consumer_logger = logging.getLogger("airflow")

# get Kafka configuration information
connection_config = {
    "bootstrap.servers": "rc1b-0qd2fn83sq9vp78r.mdb.yandexcloud.net:9091",
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "SCRAM-SHA-512",
    "sasl.username": "jupiter-user",
    "sasl.password": "pass1234",
    "ssl.ca.location": "/tmp/YandexCA.crt"
}

def consumer_function(message, prefix=None):
    key = json.loads(message.key())
    value = json.loads(message.value())
    consumer_logger.info(
        f"{prefix} {message.topic()} @ {message.offset()}; {key} : {value}"
    )
    return

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
    
    consumer_task = ConsumeFromTopicOperator(
        task_id="consume_from_topic",
        topics=["jupiter"],
        apply_function=functools.partial(consumer_function, prefix="consumed:::"),
        consumer_config={
            "bootstrap.servers": "rc1b-0qd2fn83sq9vp78r.mdb.yandexcloud.net:9091",
            "group.id": "foo",
            "enable.auto.commit": False,
            "auto.offset.reset": "beginning",
        },
        commit_cadence="end_of_batch",
        max_messages=30,
        max_batch_size=10,
    )
    
    producer_task >> consumer_task
