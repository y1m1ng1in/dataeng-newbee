#!/usr/bin/env python3
# import dependence
from confluent_kafka import Producer, Consumer, KafkaError
import json
from datetime import date
import logging

from constants import CONFIG
from helper import find_data, load_logger

delivered_records = 0


def acked(err, msg):
    global delivered_records
    """Delivery report handler called on
    successful or failed delivery of message
    """
    if err is not None:
        logging.error("Failed to deliver message: {}".format(err))
    else:
        delivered_records += 1
        logging.info(
            "Produced record to topic {} partition [{}] @ offset {}".format(
                msg.topic(), msg.partition(), msg.offset()
            )
        )


def produce(config, topic, data):
    # construct producer
    producer = Producer(config)

    for one in data:
        # prepare message
        record_key = str(date.today())
        record_value = json.dumps(one)
        logging.info("Producing record: {}\t{}".format(record_key, record_value))
        producer.produce(topic, key=record_key, value=record_value, on_delivery=acked)
        # from previous produce() calls.
        producer.poll(timeout=1)

    producer.flush()

    # show record
    logging.info(
        "{} messages were produced to topic {}!".format(delivered_records, topic)
    )


def main():
    topic = "c-tran"
    load_logger("producer", if_file=True, if_stream=True)

    # load, and produce json data.
    data_list = find_data(day="2021-01-16")
    logging.info(data_list)
    for i in data_list:
        with open(i) as json_file:
            data = json.load(json_file)
        produce(config=CONFIG, topic=topic, data=data)


if __name__ == "__main__":
    main()
