#!/usr/bin/env python3
# import dependence
from confluent_kafka import Producer
import json
from datetime import date
import logging
from multiprocessing import Process

from constants import CONFIG, BREADCRUMBS_DIR, EVENT_STOP_DIR
from helper import find_data, load_logger


class NewbeeProducer(Process):
    """The producer used by newbee group and send two types of messages."""

    def __init__(self,
                 topic='',
                 config=CONFIG,
                 day=str(date.today()),
                 data_dir='',):
        Process.__init__(self)
        self.delivered_records = 0
        self.topic = topic
        self.config = config
        self.day = day
        self.data_dir = data_dir

    def acked(self, err, msg):
        """Delivery report handler called on
        successful or failed delivery of message
        """
        if err is not None:
            logging.error("Failed to deliver message: {}".format(err))
        else:
            self.delivered_records += 1
            logging.info(
                "Produced record to topic {} partition [{}] @ offset {}".format(
                    msg.topic(), msg.partition(), msg.offset()
                )
            )

    def produce(self, data):
        # construct producer
        producer = Producer(self.config)

        for one in data:
            # prepare message
            record_key = str(date.today())
            record_value = json.dumps(one)
            # logging.info("Producing record: {}\t{}".format(record_key, record_value))
            producer.produce(self.topic, key=record_key, value=record_value, on_delivery=self.acked)
            # from previous produce() calls.
            producer.poll(timeout=0)

        producer.flush()

        # show record
        logging.info(
            "{} messages were produced to topic {}!".format(self.delivered_records, self.topic)
        )

    def run(self):
        data = find_data(self.day, dir=self.data_dir)

        # load, and produce json data.
        logging.info(data)
        with open(data) as json_file:
            data = json.load(json_file)
        self.produce(data=data)


def run():
    """methods only for every day running."""
    load_logger('producer', if_file=False)
    plist = [NewbeeProducer(topic='c-tran', data_dir=BREADCRUMBS_DIR),
             NewbeeProducer(topic='cad-avl', data_dir=EVENT_STOP_DIR)]

    for p in plist:
        p.start()

    for p in plist:
        p.join()


if __name__ == "__main__":
    load_logger('producer', if_file=False)
    plist = []
    plist.append(NewbeeProducer(topic='c-tran', config=CONFIG, data_dir=BREADCRUMBS_DIR))
    plist.append(NewbeeProducer(topic='cad-avl', config=CONFIG, data_dir=EVENT_STOP_DIR))

    for p in plist:
        p.start()

    for p in plist:
        p.join()
