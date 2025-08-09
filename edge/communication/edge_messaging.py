import base64
import json
import os
import threading
import time
import pika
import paho.mqtt.client as mqtt

from shared.logging_config import logger
from edge.communication.edge_service import EdgeService
from shared.node_state import FederatedNodeState


class EdgeMessaging:
    def __init__(self,
                 fog_amqp_host: str = os.getenv('FOG_RABBITMQ_HOST', 'rabbitmq-fog1'),
                 fog_mqtt_host: str = os.getenv('FOG_MQTT_HOST', 'mqtt-fog1'),
                 fog_mqtt_port: int = int(os.getenv('FOG_MQTT_PORT', 1883)),
                 edge_service: EdgeService = None):
        """
        :param fog_amqp_host: hostname or IP of the fog’s RabbitMQ broker.
        :param fog_mqtt_host: hostname or IP of the fog's MQTT broker.
        :param fog_mqtt_port: port of the fog's MQTT broker.
        """
        self.fog_amqp_host = fog_amqp_host
        self.fog_mqtt_host = fog_mqtt_host
        self.fog_mqtt_port = fog_mqtt_port
        self.edge_service = edge_service

    def _create_connection(self, retries=10, delay=5) -> pika.BlockingConnection:
        """Helper to create a new RabbitMQ connection with retry logic for fog node."""
        for attempt in range(1, retries + 1):
            try:
                connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.fog_amqp_host))
                return connection
            except pika.exceptions.AMQPConnectionError as e:
                logger.warning(f"Fog AMQP connection failed (attempt {attempt}/{retries}): {e})")
                if attempt == retries:
                    logger.error(
                        f"Could not connect to RabbitMQ broker ({self.fog_amqp_host}) after {retries} retries.")
                    raise
                time.sleep(delay)
        return None

    def start_amqp_listener(self):
        connection = self._create_connection()
        channel = connection.channel()
        channel.basic_qos(prefetch_count=1)  # be nice to the broker

        queue_name = f'edge_{FederatedNodeState.get_current_node().name}_messages_queue'
        channel.queue_declare(queue=queue_name, durable=True)

        def on_amqp_command(ch, method, _props, body):
            try:
                msg = json.loads(body.decode("utf-8") if isinstance(body, (bytes, bytearray)) else body)
            except Exception as e:
                logger.exception(f"Edge: failed to parse AMQP message: {e}")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            cmd = msg.get('command')
            # accept "2" or 2
            if str(cmd) == '2':
                logger.info(
                    f"Edge {FederatedNodeState.get_current_node().name}: received AMQP retrain/broadcast model command.")
                try:
                    self.edge_service.retrain_fog_model(msg)
                    logger.info(f"Edge {FederatedNodeState.get_current_node().name}: retrained local model.")
                except Exception as e:
                    logger.exception(f"Edge: retrain_fog_model failed: {e}")
            else:
                logger.debug(f"Edge: AMQP message ignored (command={cmd!r}).")

            ch.basic_ack(delivery_tag=method.delivery_tag)

        channel.basic_consume(queue=queue_name, on_message_callback=on_amqp_command, auto_ack=False)
        threading.Thread(target=channel.start_consuming, daemon=True).start()

    def start_mqtt_listener(self, retries=10, delay=5) -> None:
        """Listen for commands from the fog and dispatch them to the EdgeService."""
        # MQTT retry logic + port usage
        mqtt_client = mqtt.Client()

        def on_mqtt_command(_client, _userdata, msg):
            try:
                payload = json.loads(msg.payload.decode())
                command = payload.get('command')
                if command == '0':
                    logger.info(
                        f"Edge {FederatedNodeState.get_current_node().name}: received command to create a local model.")
                    EdgeService.create_local_edge_model()
                elif command == '1':
                    logger.info(
                        f"Edge {FederatedNodeState.get_current_node().name}: received command to train local model.")
                    self.edge_service.train_edge_local_model(payload)
            except Exception as e:
                logger.exception(f"Edge: error while handling MQTT command: {e}")

        mqtt_client.on_message = on_mqtt_command

        for attempt in range(1, retries + 1):
            try:
                mqtt_client.connect(self.fog_mqtt_host, self.fog_mqtt_port)
                break
            except Exception as e:
                logger.warning(f"MQTT connection failed (attempt {attempt}/{retries}): {e}")
                if attempt == retries:
                    logger.error(f"Could not connect to MQTT broker ({self.fog_mqtt_host}:{self.fog_mqtt_port}) after {retries} retries.")
                    raise
                time.sleep(delay)

        mqtt_client.subscribe(f'fog/{FederatedNodeState.get_current_node().name}/command', qos=1)
        logger.info(f"Edge {FederatedNodeState.get_current_node().name}: subscribed to MQTT commands on fog/{FederatedNodeState.get_current_node().name}/command.")
        mqtt_client.loop_forever()

    def send_trained_model(self, model_path: str, metrics: dict) -> None:
        """Send a trained model and its metrics back to the fog."""
        # Read the model file and encode as base64
        with open(model_path, "rb") as f:
            model_bytes = f.read()
        model_b64 = base64.b64encode(model_bytes).decode('utf-8')

        payload = {
            "edge_mac": FederatedNodeState.get_current_node().device_mac,
            "edge_name": FederatedNodeState.get_current_node().name,
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
        logger.info(f"Edge {FederatedNodeState.get_current_node().name}: sent trained model and metrics to fog.")
