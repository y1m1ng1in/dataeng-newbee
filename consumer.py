#!/usr/bin/env python3
from confluent_kafka import Consumer
import logging
import json
from helper import load_logger
from validation import Validator
from load_inserts import connect, formsql, insert, insert_batch

from constants import CONFIG, MAX_RECORDS


def consume(config, topic, group_id="newbee", auto_offset_reset="earliest"):
    # complete consumer
    config["group.id"] = group_id
    config["auto.offset.reset"] = auto_offset_reset
    validator = Validator(None)

    # database related
    records = []
    conn = connect()
    wait_time = 0

    # construct consumer.
    consumer = Consumer(config)
    consumer.subscribe([topic])
    total_count = 0
    saved_count = 0

    try:
        while True:
            msg = consumer.poll(1)

            if msg is None:
                # logging.warning("Waiting for message or event/error in poll()")
                if wait_time <= 10:
                    wait_time += 1
                    continue
                else:
                    if len(records) == 0:
                        logging.info('set')
                    insert_batch(conn, records)
                    #insert(conn, cmd=formsql(records))
                    records = []
                    wait_time = 0
                    continue
            elif msg.error():
                logging.error("error: {}".format(msg.error()))
            else:
                total_count += 1
                record_key    = msg.key()
                record_value  = msg.value()
                record_offset = msg.offset()
                record_str    = record_value.decode('utf-8')
                j = json.loads(record_str)
                j, should_save = validator.validate(j)
                if should_save:
                    # prepare to add
                    records.append(j)
                    saved_count += 1

                logging.info(
                    "Consumed record with key {} and value {}.".format(
                        msg.key(), msg.value(),
                    )
                )

    except KeyboardInterrupt:
        pass
    finally:
        logging.info("Total consume {} messages.".format(total_count))
        consumer.close()

    return records


def main():
    load_logger("consumer", if_file=True, if_stream=True)

    record = consume(config=CONFIG, topic="c-tran")


if __name__ == "__main__":
    main()
