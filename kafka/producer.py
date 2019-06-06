from datetime import datetime
from collections.abc import Iterable

import confluent_kafka
from config import get_kafka_args
from db.models import WeiboData
from logger import crawler

conf = get_kafka_args()
data_type_dict = {'dataset': 0, 'datastream': 1, 'keyword': 2, 'home': 3}


def produce_data_list(topic: str, data_type: str, data_id: int, data_list: Iterable, area: str = ''):
    p = confluent_kafka.Producer(conf)
    for weibo_item in data_list:
        produce_data(p, topic, data_type, data_id, weibo_item, area)
    kafka_flush(p)


def produce_data(p, topic: str, data_type: str, data_id: int, data: WeiboData, area: str = ''):
    p.poll(0)
    p.produce(topic, __serialize(data_type_dict[data_type], data_id, data, area), callback=delivery_report)


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        crawler.error(f'Kafka message delivery failed: {err}')
    else:
        crawler.info(f'Kafka message delivered to {msg.topic()} [{msg.partition()}]')


def kafka_flush(p):
    return p.flush()


def __serialize(data_type: int, data_id: int, weibo_item: WeiboData, area=''):
    mid_bytes = bytes(weibo_item.weibo_id, encoding='utf-8')
    content_bytes = bytes(weibo_item.weibo_cont, encoding='utf-8')
    location_bytes = bytes(weibo_item.weibo_location, encoding='utf-8')
    area_bytes = bytes(area, encoding='utf-8')
    time = int(datetime.timestamp(weibo_item.create_time) * 1000)
    time_bytes = time.to_bytes(length=8, byteorder='big')
    data_bytes = [data_type.to_bytes(length=1, byteorder='big'),
                  (data_id.to_bytes(length=4, byteorder='big')),
                  (len(content_bytes).to_bytes(length=2, byteorder='big')),
                  (len(location_bytes).to_bytes(length=2, byteorder='big')),
                  (len(area_bytes).to_bytes(length=1, byteorder='big')),
                  mid_bytes,
                  content_bytes,
                  location_bytes,
                  area_bytes,
                  time_bytes]
    return b''.join(data_bytes)


if __name__ == '__main__':
    weibo_item_test = WeiboData()
    weibo_item_test.weibo_cont = '#地震预警# 据成都高新减灾研究所，15:57 四川芦山(103.0,' \
                                 '30.4)发生预警震级4.5级的地震，预警中心提前12秒向成都预警，预估烈度2.0度。此预警信息于震中发震后几秒内发出，最终地震参数以7-10' \
                                 '分钟后中国地震台网发布的正式数据为准。用户可下载“地震预警”(ICL)手机软件接收地震预警。 \u200B\u200B\u200B\u200B'
    weibo_item_test.weibo_location = ''
    weibo_item_test.weibo_id = '4367530648576413'
    weibo_item_test.create_time = datetime.fromtimestamp(1556783914)
    s = __serialize(2, 1, weibo_item_test)
    h = s.hex()
    print(len(s))
    print(h)
