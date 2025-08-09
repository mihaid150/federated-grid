import base64
import json
import os
import time
from typing import Dict

import pika
import paho.mqtt.client as mqtt
from shared.logging_config import logger
from shared.node_state import FederatedNodeState
from cloud.model.model_aggregation_service import aggregate_received_models
from cloud.communication.cloud_resources_paths import CloudResourcesPaths


class CloudMessaging:
    def __init__(
            self,
            cloud_amqp_host: str = os.getenv('CLOUD_RABBITMQ_HOST', 'rabbitmq-cloud'),
            cloud_mqtt_host: str = os.getenv('CLOUD_MQTT_HOST', 'mqtt-cloud'),
            cloud_mqtt_port: int = int(os.getenv('CLOUD_MQTT_PORT', 1883)),
    ):
        """
        :param cloud_amqp_host: hostname or IP of the cloud’s RabbitMQ broker.
        :param cloud_mqtt_host: hostname or IP of the cloud's MQTT broker.
        :param cloud_mqtt_port: port of the cloud's MQTT broker.
        """
        self.cloud_amqp_host = cloud_amqp_host
        self.cloud_mqtt_host = cloud_mqtt_host
        self.cloud_mqtt_port = cloud_mqtt_port
        # Cache for aggregated models keyed by fog ID
        self.fog_models_cache = {}

    def _create_connection(self, retries=10, delay=5) -> pika.BlockingConnection:
        """Helper to create a new RabbitMQ connection with retry logic for cloud node."""
        for attempt in range(1, retries + 1):
            try:
                connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.cloud_amqp_host))
                return connection
            except pika.exceptions.AMQPConnectionError as e:
                logger.warning(f"Cloud AMQP connection failed (attempt {attempt}/{retries}): {e})")
                if attempt == retries:
                    logger.error(
                        f"Could not connect to RabbitMQ broker ({self.cloud_amqp_host}) after {retries} retries.")
                    raise
                time.sleep(delay)
        return None

    def _mqtt_publish(self, topic: str, message: dict, qos: int = 1, retries=10, delay=5):
        client = mqtt.Client()
        for attempt in range(1, retries + 1):
            try:
                client.connect(self.cloud_mqtt_host, self.cloud_mqtt_port)
                break
            except Exception as e:
                logger.warning(f"Cloud MQTT connection failed (attempt {attempt}/{retries}): {e}")
                if attempt == retries:
                    logger.error(
                        f"Cloud: Could not connect to MQTT broker ({self.cloud_mqtt_host}:{self.cloud_mqtt_port}) after {retries} retries.")
                    return
                time.sleep(delay)
        client.publish(topic, json.dumps(message), qos=qos)
        client.disconnect()

    def notify_all_edges_to_create_local_model(self):
        """Use MQTT to instruct all edges (via fogs) to create a local model."""
        self._mqtt_publish(topic='cloud/fog/command', message={'command': '0'})
        logger.info("Cloud (MQTT): sent command to fogs instructing edges to create local model.")

    def notify_all_edges_to_start_first_training(self, data: Dict[str, any]):
        """Use MQTT to instruct all edges (via fogs) to start the first training."""
        self._mqtt_publish(topic='cloud/fog/command', message={'command': '1', 'data': data})
        logger.info("Cloud (MQTT): sent command to fogs instructing edges to start the first training.")

    def broadcast_cloud_model(self, data: Dict[str, any]):
        """
        Use RabbitMQ to broadcast an aggregated cloud model (binary payload) to fogs.
        This remains AMQP because model files are large.
        """

        self.fog_models_cache.clear()

        model_path = CloudResourcesPaths.CLOUD_MODEL_FILE_PATH.value
        if not os.path.exists(model_path):
            logger.error("Cloud: no aggregated model found at %s, cannot broadcast.", model_path)
            return

        with open(model_path, "rb") as f:
            model_b64 = base64.b64encode(f.read()).decode('utf-8')

        message = {"command": "2", "model": model_b64, "data": data}
        message_body = json.dumps(message).encode('utf-8')

        connection = self._create_connection()
        channel = connection.channel()
        channel.exchange_declare(exchange='cloud_fog_exchange', exchange_type='fanout', durable=True)
        channel.basic_publish(
            exchange='cloud_fog_exchange',
            routing_key='',
            body=message_body,
            properties=pika.BasicProperties(delivery_mode=2),
        )
        logger.info("Cloud (AMQP): broadcast cloud model to fogs.")
        connection.close()

    def start_fog_model_listener(self):
        """
        Listen for aggregated models from fogs and cache them.
        """
        os.makedirs(CloudResourcesPaths.MODELS_FOLDER_PATH.value, exist_ok=True)
        connection = self._create_connection()
        channel = connection.channel()
        channel.queue_declare(queue='fog_to_cloud_models', durable=True)

        def on_fog_model(ch, method, _properties, body):
            payload = json.loads(body)
            fog_device_mac = payload.get('fog_device_mac')
            fog_name = payload.get('fog_name')
            model_b64 = payload.get('model')

            if not fog_device_mac or not model_b64:
                logger.warning("Cloud: received malformed message from fog; missing fog_device_mac or model.")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            try:
                model_bytes = base64.b64decode(model_b64)
                model_path = os.path.join(CloudResourcesPaths.MODELS_FOLDER_PATH.value, f'{fog_name}_aggregated_model.keras')
                with open(model_path, 'wb') as f:
                    f.write(model_bytes)
                map_id = fog_device_mac + '_' + fog_name
                self.fog_models_cache[map_id] = {"model_path": model_path}
                logger.info(f"Cloud: cached aggregated model from fog {fog_name} at {model_path}.")

            except Exception as e:
                logger.error(f"Cloud: failed to decode or save model from fog {fog_name}: {e}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            self.is_ready_to_aggregate()

        channel.basic_consume(
            queue='fog_to_cloud_models', on_message_callback=on_fog_model, auto_ack=False
        )
        logger.info("Cloud: listening for aggregated models from fogs...")
        channel.start_consuming()

    def is_ready_to_aggregate(self):
        if len(self.fog_models_cache) == len(FederatedNodeState.get_current_node().child_nodes):
            logger.info("Cloud has received all fog models and is ready to aggregate them.")
            aggregate_received_models(self.fog_models_cache)
            self.fog_models_cache.clear()
            logger.info("Cloud has succeeded to aggregate fog models and obtained cloud model.")
