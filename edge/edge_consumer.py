import json
import pika
from shared.logging_config import logger
from edge.edge_service import EdgeService


def start_edge_consumer(edge_name):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='FOG_RABBITMQ_HOST')
    )
    channel = connection.channel()

    # declare the queue that this edge listens on and look for fog's routing key
    queue_name = f'edge_{edge_name}_create_model'
    channel.queue_declare(queue=queue_name, durable=True)

    def on_fog_message(ch, method, _, body):
        msg = json.loads(body)

        if msg.get('command') == '0':
            logger.info(f"Edge {edge_name}: has received command to create a local model.")
            EdgeService.create_local_edge_model()
            logger.info(f"Edge {edge_name}: has created a local model.")
        elif msg.get('command') == '1':
            logger.info(f"Edge {edge_name}: has received command to train local model.")
            EdgeService.train_edge_local_model(msg)
            logger.info(f"Edge {edge_name}: has succeeded to train local edge model.")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(
        queue=queue_name, on_message_callback=on_fog_message, auto_ack=False
    )

    logger.info(f"Edge {edge_name}: waiting for other fog commands...")
    channel.start_consuming()