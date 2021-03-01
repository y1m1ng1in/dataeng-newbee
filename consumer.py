#!/usr/bin/env python3
from confluent_kafka import Consumer
import logging
import json
from helper import load_logger
from load_inserts import connect, insert_batch
from collector import Collector

from constants import CONFIG


class NewbeeConsumer:
    """The consumer for all newbee's data."""

    def __init__(self,
                 topic: list,
                 config=CONFIG,
                 group_id='newbee',
                 offset_reset='earliest',):
        self.config = config
        self.config['group.id'] = group_id
        self.config['auto.offset.reset'] = offset_reset
        self.consumer = Consumer(self.config)

        self.topic = topic
        self.conn = connect()
        self.collector = Collector()

    def consume(self):
        # construct consumer.
        self.consumer.subscribe(self.topic)
        total_count = 0
        wait = 0

        try:
            while True:
                msg = self.consumer.poll(1)

                if msg is None:
                    # logging.warning("Waiting for message or event/error in poll()")
                    if wait < 5:
                        wait += 1
                    else:
                        insert_batch(self.conn, self.collector.integrate())
                        wait = 0
                elif msg.error():
                    logging.error("error: {}".format(msg.error()))
                else:
                    total_count += 1
                    record_value = msg.value()
                    record_str = record_value.decode('utf-8')
                    j = json.loads(record_str)
                    self.collector.receive(j)

                    logging.info(
                        "Consumed record with key {} and value {}.".format(
                            msg.key(), msg.value(),
                        )
                    )

        except KeyboardInterrupt:
            pass
        finally:
            logging.info("Total consume {} messages.".format(total_count))
            self.consumer.close()


def main():
    load_logger("consumer", if_file=True)

    consumer = NewbeeConsumer(topic=['c-tran', 'cad-avl'])
    consumer.consume()


if __name__ == "__main__":
    main()
