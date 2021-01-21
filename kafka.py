#!/usr/bin/env python3
# import dependence
from confluent_kafka import Producer, Consumer, KafkaError
import json
from datetime import datetime

from constants import config, breadcrumbs_dir
from helper import find_data


config = {
    'bootstrap.servers': 'pkc-lgk0v.us-west1.gcp.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'OS6ZO4C7D74HPYXP',
    'sasl.password': 'dOqPALDZ/G7Oe/+6SHNjX3hrQjVGm+XKj4FmfE6V757Zse7TCFmFQCdzv0RBriOy',
}

delivered_records = 0


def acked(err, msg):
    global delivered_records
    """Delivery report handler called on
    successful or failed delivery of message
    """
    delivered_records += 1
    print(err)
    print("Produced record to topic {} partition [{}] @ offset {}".format(msg.topic(), msg.partition(), msg.offset()))


def produce(topic, data):
    # construct producer
    producer = Producer(config)

    for one in data:
        # prepare message
        record_key = str(datetime.now())
        print(one)
        record_value = json.dumps(one)
        print("Producing record: {}\t".format(record_key))
        producer.produce(topic, key=record_key, value=record_value, on_delivery=acked)
        # p.poll() serves delivery reports (on_delivery)
        # from previous produce() calls.
        producer.poll(2)

    producer.flush()

    # show record
    print(delivered_records)


def main():
    topic = 'C-Tran'
    data_list = find_data(breadcrumbs_dir)

    for i in data_list:
        with open(i) as json_file:
            data = json.load(json_file)
        produce(topic=topic, data=data)
    
    consume(config=config, topic=topic)


def consume(config, topic, group_id = 'example', auto_offset_reset = 'earliest'):
    # complete consumer
    config['group.id'] = group_id
    config['auto.offset.reset'] = auto_offset_reset

    # construct consumer.
    consumer = Consumer(config)
    consumer.subscribe([topic])
    total_count = 0

    while True:
        msg = consumer.poll(1)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break

        # record_key = msg.key()
        # record_value = msg.value()
        data = json.loads(msg.value())
        total_count += 1
        print("Consumed record with key {} and value {}, and updated total count to {}"
                      .format(msg.key(), msg.value(), total_count))
    
    consumer.close()


if __name__ == '__main__':
    main()

