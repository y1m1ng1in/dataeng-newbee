#!/usr/bin/env python3
# import dependence
from confluent_kafka import Producer, Consumer, KafkaError
import json
from datetime import date
import logging
import os

from constants import produce_config as config
from helper import find_data


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


def produce(topic, data):
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
    topic = "C-Tran"

    # set log file path
    log_path = "/home/yl6/dataeng-project/log"
    os.makedirs(log_path, exist_ok=True)
    log_path = os.path.join(log_path, "{}_producer.log".format(date.today()))

    # set loggingfile
    logging.basicConfig(
        filename=log_path,
        level=logging.INFO,
        format="%(message)s",
    )

    # load, and produce json data.
    data_list = find_data()
    logging.info(data_list)
    for i in data_list:
        with open(i) as json_file:
            data = json.load(json_file)
        produce(topic=topic, data=data)

    # consume(config=config, topic=topic)


def consume(config, topic, group_id="newbee", auto_offset_reset="earliest"):
    # complete consumer
    config["group.id"] = group_id
    config["auto.offset.reset"] = auto_offset_reset

    # construct consumer.
    consumer = Consumer(config)
    consumer.subscribe([topic])
    total_count = 0

    while True:
        msg = consumer.poll(1)

        if msg is None:
            logging.warning("Waiting for message or event/error in poll()")
            continue
        elif msg.error():
            logging.error("error: {}".format(msg.error()))
        else:
            data = json.loads(msg.value())
            total_count += 1
            logging.info(
                "Consumed record with key {} and value {}, and updated total count to {}".format(
                    msg.key(), msg.value(), total_count
                )
            )

    consumer.close()


if __name__ == "__main__":
    main()
