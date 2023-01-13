import collections
import heapq
import time
from asyncio import new_event_loop, set_event_loop, get_event_loop, ensure_future, gather
from datetime import datetime
from json import loads
from heapq import heappop, heappush
from pandas import DataFrame
from threading import Thread, Lock, Event
from traceback import extract_tb, format_exc
from kafka import KafkaConsumer, TopicPartition
import os
import yaml

from sys import exc_info, platform
from kafka import KafkaConsumer, KafkaProducer, TopicPartition


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



class Consumer(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.stop_event = Event()

        # self.qs = qs
        # self.qs_key = self.qs.keys()

    def stop(self):
        self.stop_event.set()

    def _check_subscription(self, ticker):
        """
        Checks if the ticker is in the subscription list.\n
        :param ticker: Ticker of product
        :return: Boolean status whether subscription is completed.
        """
        if ticker not in self.qs_key:
            self.qs[ticker] = []
            self.qs_key = self.qs.keys()
            # logger.info(self.qs_key)
            print(self.qs_key)

        return True

    async def consume(self, kafka_cfg):
        """
        Consumes data from Kafka with topic.\n
        :param topic: Kafka topic
        """
        if platform.startswith('linux'):
            consumer = KafkaConsumer(
                bootstrap_servers=kafka_cfg["ip"],
                auto_offset_reset=kafka_cfg["offset"],
                security_protocol=kafka_cfg['security_protocol'],
                sasl_mechanism=kafka_cfg["sasl_mechanism"],
                sasl_plain_username=kafka_cfg["username"],
                sasl_plain_password=kafka_cfg["password"],
                ssl_cafile=kafka_cfg["ca_file"],
                client_id=kafka_cfg["username"],
                group_id=kafka_cfg["group"],
            )
        else:
            consumer = KafkaConsumer(
                bootstrap_servers=kafka_cfg["ip"],
                auto_offset_reset=kafka_cfg["offset"],
            )
        topic = "topictest1"
        consumer.assign([TopicPartition(topic, 0)])
        consumer.poll()
        consumer.seek_to_end()

        try:
            for msg in consumer:
                try:
                    print(loads(msg.value))
                except KeyError:
                    continue
        except Exception as e:
            logger.exception('Kakfa Consumer Error')
            print(f"Unknown kafka Error: {str(e)}")
        finally:
            # Will leave consumer group; perform autocommit if enabled.
            await consumer.close()
            msg = "Consumer is stopped"
            print(msg)

    def run(self, kwargs):
        """
        Runs Kafka consumer.\n
        """


        # 建立一個Event Loop
        new_loop = new_event_loop()
        set_event_loop(new_loop)
        loop = get_event_loop()

        # 建立一個任務列表
        tasks = [
            ensure_future(self.consume(kwargs))
        ]

        # 開始執行
        loop.run_until_complete(gather(*tasks))


if __name__ == '__main__':
    consumer = Consumer()
    consumer.run(load_config()['kafka'])