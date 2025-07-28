import json
import pika
from shared.logging_config import logger


def notify_all_edges_to_create_local_model():
    # connect to cloud's rabbitmq broker
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='CLOUD_RABBITMQ_HOST')  # adapt
    )
    channel = connection.channel()

    # declare a durable fanout exchange, thus every fog will bind a queue to this exchange
    channel.exchange_declare(
        exchange='cloud_fog_create_model', exchange_type='fanout', durable=True
    )

    message = {'command': '0'}
    message_body_str = json.dumps(message)
    message_body_bytes = message_body_str.encode('utf-8')

    # publish the message persistently so it survives if broker restarts
    channel.basic_publish(
        exchange='cloud_fog_create_model',
        routing_key='',  # fanout ignores routing keys
        body=message_body_bytes,
        properties=pika.BasicProperties(delivery_mode=2),  # persistent
    )

    logger.info("Cloud has sent the command to fogs for telling edges to create local model.")

    connection.close()