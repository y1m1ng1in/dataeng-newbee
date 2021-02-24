#!/usr/bin/env python3
from confluent_kafka import Consumer
import logging
import json
from helper import load_logger
from validation import Validator
from load_inserts import connect, formsql, insert, insert_batch

from constants import CONFIG, MAX_RECORDS


class NewbeeConsumer:
    """The consumer for all newbee's data."""

    def __init__(self,
                 topic: list,
                 batch_size=MAX_RECORDS,
                 config=CONFIG,
                 group_id='newbee',
                 offset_reset='earliest',):
        self.config = config
        self.config['group.id'] = group_id
        self.config['auto.offset.reset'] = offset_reset
        self.consumer = Consumer(self.config)

        self.topic = topic
        self.conn = connect()
        self.batch = list()
        self.batch_size = batch_size
        self.records = []
        self.validator = Validator(None)

    def consume(self):
        # construct consumer.
        self.consumer.subscribe(self.topic)
        total_count = 0
        saved_count = 0

        try:
            while True:
                msg = self.consumer.poll(1)

                if msg is None:
                    # logging.warning("Waiting for message or event/error in poll()")
                    pass
                elif msg.error():
                    logging.error("error: {}".format(msg.error()))
                else:
                    total_count += 1
                    # record_key    = msg.key()
                    record_value = msg.value()
                    # record_offset = msg.offset()
                    record_str = record_value.decode('utf-8')
                    j = json.loads(record_str)
                    j, should_save = self.validator.validate(j)
                    if should_save:
                        # prepare to add
                        self.records.append(j)
                        saved_count += 1

                    if len(self.records) >= self.batch_size:
                        self.clean_batch(self.records)

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

        return self.records

    def clean_batch(self, batch: list):
        insert_num = len(batch)
        insert_batch(self.conn, batch)
        self.records = []
        logging.info(f'{insert_num} records inserted to {self.topic}.')


def main():
    load_logger("consumer", if_file=True)

    consumer = NewbeeConsumer(topic=['c-tran', 'cad-avl'])
    consumer.consume()


if __name__ == "__main__":
    main()
