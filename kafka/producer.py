from datetime import datetime
from collections import Iterable

import confluent_kafka
from config import get_kafka_args
from db.models import WeiboData

conf = get_kafka_args.__dict__
p = confluent_kafka.Producer(**conf)


def produce_data_list(topic: str, data_list: Iterable, province_city_id: str = ''):
    map(lambda d: produce_data(topic, d, province_city_id), data_list)


def produce_data(topic: str, data: WeiboData, province_city_id: str = ''):
    p.poll(0)
    p.produce(topic, __serialize(data, province_city_id), callback=delivery_report)


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Kafka message delivery failed: {}'.format(err))
    else:
        print('Kafka message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


def kafka_flush():
    p.flush()


def __serialize(weibo_item: WeiboData, area=''):
    time = int(datetime.timestamp(weibo_item.create_time) * 1000)
    # bytes([(time >> 56) % 256, (time >> 48) % 256, (time >> 40) % 256, (time >> 32) % 256
    #        , (time >> 24) % 256, (time >> 16) % 256, (time >> 8) % 256, time % 256])
    return len(weibo_item.weibo_cont).to_bytes(length=2, byteorder='big') \
        .join(len(weibo_item.weibo_location).to_bytes(length=2, byteorder='big')) \
        .join(len(area).to_bytes(length=1, byteorder='big')) \
        .join(bytes(weibo_item.weibo_id, encoding='utf-8')) \
        .join(bytes(weibo_item.weibo_cont, encoding='utf-8')) \
        .join(bytes(weibo_item.weibo_location, encoding='utf-8')) \
        .join(bytes(area, encoding='utf-8')) \
        .join(time.to_bytes(length=8, byteorder='big'))
