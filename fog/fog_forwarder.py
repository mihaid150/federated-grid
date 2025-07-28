import json
import pika
from shared.logging_config import logger


def start_fog_forwarder(edge_ids):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='FOG_RABBITMQ_HOST')
    )
    channel = connection.channel()

    # ensure the same fanout exchange exists on the fog broker
    channel.exchange_declare(
        exchange='cloud_fog_create_model', exchange_type='fanout', durable=True
    )

    # create an exclusive queue for this fog and bind it to the fanout exchange
    result = channel.queue_declare(queue='', exclusive=True)
    fog_queue = result.method.queue
    channel.queue_bind(exchange='cloud_fog_create_model', queue=fog_queue)

    def on_cloud_message(ch, method, properties, body):
        msg = json.loads(body)
        if msg.get('command') == '0':
            logger.info("Fog has received create local model edge command.")

            for edge_id in edge_ids:
                edge_queue = f'edge_{edge_id}_create_model'
                channel.queue_declare(queue=edge_queue, durable=True)
                channel.basic_publish(
                    exchange='',  # direct to queue
                    routing_key=edge_queue,
                    body=body,
                    properties=pika.BasicProperties(delivery_mode=2)
                )
                logger.info(f"Fog has forwarded to edge {edge_id}.")

        # acknowledge the cloud message such that it can be removed from the queue
        ch.basic_ack(delivery_tag=method.delivery_tag)

    # consume messages from the fog's bound queue
    channel.basic_consume(
        queue=fog_queue, on_message_callback=on_cloud_message, auto_ack=False
    )
    logger.info("Fog is waiting for other cloud commands...")
    channel.start_consuming()