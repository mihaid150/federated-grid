import base64
import json
import pika
from shared.logging_config import logger
from edge.communication.edge_service import EdgeService


class EdgeMessaging:
    def __init__(self, edge_name: str, edge_mac: str, fog_host: str = 'FOG_RABBITMQ_HOST'):
        """
        :param edge_name: human-readable name of the edge node
        :param edge_mac: unique identifier (MAC or similar) used in queue naming
        :param fog_host: hostname or IP of the fog’s RabbitMQ broker
        """
        self.edge_name = edge_name
        self.edge_mac = edge_mac
        self.fog_host = fog_host

    def _create_connection(self) -> pika.BlockingConnection:
        """Helper to create a new RabbitMQ connection."""
        return pika.BlockingConnection(pika.ConnectionParameters(host=self.fog_host))

    def start_consumer(self) -> None:
        """Listen for commands from the fog and dispatch them to the EdgeService."""
        connection = self._create_connection()
        channel = connection.channel()

        # Each edge listens on its own queue
        queue_name = f'edge_{self.edge_mac}_messages_queue'
        channel.queue_declare(queue=queue_name, durable=True)

        def on_fog_message(ch, method, _, body):
            msg = json.loads(body)
            if msg.get('command') == '0':
                logger.info(f"Edge {self.edge_name}: received command to create a local model.")
                EdgeService.create_local_edge_model()
                logger.info(f"Edge {self.edge_name}: created a local model.")
            elif msg.get('command') == '1':
                logger.info(f"Edge {self.edge_name}: received command to train local model.")
                EdgeService.train_edge_local_model(msg)
                logger.info(f"Edge {self.edge_name}: trained local model.")
            elif msg.get('command') == '2':
                logger.info(f"Edge {self.edge_name}: received command to retrain fog model.")
                EdgeService.retrain_fog_model(msg)
                logger.info(f"Edge {self.edge_name}: retrained local model.")
            ch.basic_ack(delivery_tag=method.delivery_tag)

        channel.basic_consume(queue=queue_name, on_message_callback=on_fog_message, auto_ack=False)
        logger.info(f"Edge {self.edge_name}: waiting for fog commands on {queue_name}...")
        channel.start_consuming()
        # Note: channel.start_consuming() blocks indefinitely. To stop, you must close the connection from another thread.

    def send_trained_model(self, model_path: str, metrics: dict) -> None:
        """Send a trained model and its metrics back to the fog."""
        # Read the model file and encode as base64
        with open(model_path, "rb") as f:
            model_bytes = f.read()
        model_b64 = base64.b64encode(model_bytes).decode('utf-8')

        payload = {
            "edge_mac": self.edge_mac,
            "edge_name": self.edge_name,
            "model": model_b64,
            "metrics": metrics,
        }

        connection = self._create_connection()
        channel = connection.channel()
        channel.queue_declare(queue='edge_to_fog_models', durable=True)

        message_body = json.dumps(payload).encode('utf-8')
        channel.basic_publish(
            exchange='',
            routing_key='edge_to_fog_models',
            body=message_body,
            properties=pika.BasicProperties(delivery_mode=2)  # make message persistent
        )
        connection.close()
        logger.info(f"Edge {self.edge_name}: sent trained model and metrics to fog.")
