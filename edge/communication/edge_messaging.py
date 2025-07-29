import base64
import json
import threading

import pika
import paho.mqtt.client as mqtt

from shared.logging_config import logger
from edge.communication.edge_service import EdgeService
from shared.node_state import FederatedNodeState


class EdgeMessaging:
    def __init__(self, fog_amqp_host: str = 'FOG_RABBITMQ_HOST', fog_mqtt_host: str = 'FOG_MQTT_HOST',
                 edge_service: EdgeService = None):
        """
        :param fog_amqp_host: hostname or IP of the fog’s RabbitMQ broker
        :param fog_mqtt_host: hostname or IP of the fog's MQTT broker
        """
        self.edge_name = FederatedNodeState.get_current_node().name
        self.edge_mac = FederatedNodeState.get_current_node().device_mac
        self.fog_amqp_host = fog_amqp_host
        self.fog_mqtt_host = fog_mqtt_host
        self.edge_service = edge_service

    def _create_connection(self) -> pika.BlockingConnection:
        """Helper to create a new RabbitMQ connection."""
        return pika.BlockingConnection(pika.ConnectionParameters(host=self.fog_amqp_host))

    def start_amqp_listener(self):
        """Consume model update messages from the fog via AMQP in a background thread."""
        connection = self._create_connection()
        channel = connection.channel()

        queue_name = f'edge_{self.edge_mac}_messages_queue'
        channel.queue_declare(queue=queue_name, durable=True)

        def on_amqp_command(ch, method, _props, body):
            msg = json.loads(body)
            if msg.get('command') == 2:
                logger.info(f"Edge {self.edge_mac}: received command to retrain fog model via AMQP.")
                self.edge_service.retrain_fog_model(msg)
                logger.info(f"Edge {self.edge_name}: retrained local model.")

            ch.basic_ack(delivery_tag=method.delivery_tag)

        channel.basic_consume(queue=queue_name, on_message_callback=on_amqp_command, auto_ack=False)
        threading.Thread(target=channel.start_consuming, daemon=True).start()

    def start_mqtt_listener(self) -> None:
        """Listen for commands from the fog and dispatch them to the EdgeService."""
        connection = self._create_connection()
        channel = connection.channel()

        # Each edge listens on its own queue
        queue_name = f'edge_{self.edge_mac}_messages_queue'
        channel.queue_declare(queue=queue_name, durable=True)

        def on_mqtt_command(_client, _userdata, msg):
            payload = json.loads(msg.payload.decode())
            command = payload.get('command')

            if command == '0':
                logger.info(f"Edge {self.edge_name}: received command to create a local model.")
                EdgeService.create_local_edge_model()
                logger.info(f"Edge {self.edge_name}: created a local model.")
            elif command == '1':
                logger.info(f"Edge {self.edge_name}: received command to train local model.")
                self.edge_service.train_edge_local_model(payload)
                logger.info(f"Edge {self.edge_name}: trained local model.")

        mqtt_client = mqtt.Client()
        mqtt_client.on_message = on_mqtt_command
        mqtt_client.connect(self.fog_mqtt_host)
        mqtt_client.subscribe(f'fog/{self.edge_mac}/command', qos=1)
        logger.info(f"Edge {self.edge_name}: subscribed to MQTT commands on fog/{self.edge_mac}/command.")

        mqtt_client.loop_forever()

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
            properties=pika.BasicProperties(delivery_mode=2)
        )
        connection.close()
        logger.info(f"Edge {self.edge_name}: sent trained model and metrics to fog.")
