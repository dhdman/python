import queue
import yaml
import os
from datetime import datetime
from json import dumps, loads
from collections import deque, OrderedDict
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from traceback import extract_tb, format_exc
from threading import Thread
from asyncio import new_event_loop, set_event_loop, get_event_loop, ensure_future, gather
from sys import exc_info, platform

def load_config(cfg_path=None):
    """
    Loads configuration from YAML file.
    :return: Configuration
    """
    if not cfg_path:
        cfg_path = os.path.abspath(os.path.join(__file__, "../conf/property.yaml"))

    try:
        with open(cfg_path, "r") as stream:
            config = yaml.load(stream, Loader=yaml.SafeLoader)
            return config

    except Exception as ex:
        # Many possibilities to raise exceptions
        print(ex)



class Producer():
    def __init__(self):
        self.config = load_config()
        kafka_cfg = self.config['kafka']
        self.topic = "topictest1"

        if platform.startswith('linux'):
            producer = KafkaProducer(
                bootstrap_servers=kafka_cfg["ip"],
                value_serializer=lambda x: dumps(x).encode('utf-8'),
                security_protocol=kafka_cfg["security_protocol"],
                sasl_mechanism=kafka_cfg["sasl_mechanism"],
                sasl_plain_username=kafka_cfg["username"],
                sasl_plain_password=kafka_cfg["password"],
                ssl_cafile=kafka_cfg["ca_file"],
            )
        else:
            producer = KafkaProducer(
                bootstrap_servers=kafka_cfg["ip"],
                value_serializer=lambda x: dumps(x).encode('utf-8'),
            )


        self.producer = producer

    def produce(self, msg):
        self.producer.send(topic=self.topic, value=msg)

if __name__ == '__main__':
    producer = Producer()
    for i in range(100):
        producer.produce(f"this is {i}...")

