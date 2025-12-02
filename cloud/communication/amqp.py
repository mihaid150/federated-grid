import socket
import time
import random

import pika

from shared.logging_config import logger

class AmqpClient:
    def __init__(self, host: str):
        self.host = host

    def open_blocking(self, retries=10, delay=5):
        for attempt in range(1, retries + 1):
            try:
                return pika.BlockingConnection(pika.ConnectionParameters(
                    host=self.host,
                    heartbeat=30,
                    blocked_connection_timeout=60,
                    connection_attempts=1,
                    retry_delay=0,
                ))
            except (socket.gaierror, pika.exceptions.AMQPError) as e:
                if attempt == 1:
                    logger.warning("Cloud: AMQP unavailable (%s). Will retry up to %d...",
                                   e.__class__.__name__, retries)
                if attempt == retries:
                    logger.error("Cloud: could not reach RabbitMQ (%s) after %d attempts.", self.host, retries)
                    raise
                time.sleep(delay + random.uniform(0, 1.0))
                delay = min(delay * 2, 60)
        return None


def declare_durable_queue(ch, name: str, quorum: bool = False):
    args = {"x-queue-type": "quorum"} if quorum else None
    return ch.queue_declare(queue=name, durable=True, auto_delete=False, arguments=args)

def publish_to_queue(ch, queue: str, body: bytes, mandatory: bool = True):
    ch.basic_publish(
        exchange="",
        routing_key=queue,
        body=body,
        mandatory=mandatory,
        properties=pika.BasicProperties(delivery_mode=2, content_type="application/json")
    )
