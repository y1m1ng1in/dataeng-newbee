#!/usr/bin/env python3
from confluent_kafka import Consumer
import logging
from helper import load_logger

from constants import CONFIG


def consume(config, topic, group_id="newbee", auto_offset_reset="earliest"):
    # complete consumer
    config["group.id"] = group_id
    config["auto.offset.reset"] = auto_offset_reset

    # construct consumer.
    consumer = Consumer(config)
    consumer.subscribe([topic])
    total_count = 0

    try:
        while True:
            msg = consumer.poll(1)

            if msg is None:
                logging.warning("Waiting for message or event/error in poll()")
                continue
            elif msg.error():
                logging.error("error: {}".format(msg.error()))
            else:
                total_count += 1
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


def main():
    load_logger("consumer", if_file=True, if_stream=True)
    consume(config=CONFIG, topic="c-tran")


if __name__ == "__main__":
    main()
