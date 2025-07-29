import base64
import json
import os
import threading

import pika
import paho.mqtt.client as mqtt
from shared.logging_config import logger
from shared.node_state import FederatedNodeState
from fog.communication.fog_resources_paths import FogResourcesPaths
from fog.model.model_aggregation_service import aggregate_models_with_metrics


class FogMessaging:
    def __init__(self, fog_amqp_host: str = 'FOG_RABBITMQ_HOST', cloud_amqp_host: str = 'CLOUD_RABBITMQ_HOST',
                 fog_mqtt_host: str = 'FOG_MQTT_HOST'):
        """
        :param fog_amqp_host: hostname or IP of the fog's RabbitMQ broker.
        :param cloud_amqp_host: hostname or IP of the cloud's RabbitMQ broker.
        :param fog_mqtt_host: hostname or IP of the fog's MQTT broker.
        """
        self.fog_amqp_host = fog_amqp_host
        self.cloud_amqp_host = cloud_amqp_host
        self.fog_mqtt_host = fog_mqtt_host
        # Cache of received models keyed by edge ID
        self.edge_models_cache = {}

    def _create_connection(self) -> pika.BlockingConnection:
        """Helper to create a new RabbitMQ connection."""
        return pika.BlockingConnection(pika.ConnectionParameters(host=self.fog_amqp_host))

    def start_command_forwarder(self, edge_ids):
        """
        Subscribe to cloud commands over MQTT and forward them to edges.
        Large model updates from the cloud will still arrive via AMQP and
        are handled by the RabbitMQ consumer in start_model_from_cloud().
        """
        def on_mqtt_command(client, userdata, msg):
            payload = json.loads(msg.payload.decode())
            command = payload.get('command')
            if command is not None:
                # forward each command to edges over MQTT
                for edge_id in edge_ids:
                    client.publish(f'fog/{edge_id}/command', msg.payload, qos=1)
                    logger.info(f"Fog (MQTT): forwarded command {command} to edge {edge_id}.")

        mqtt_client = mqtt.Client()
        mqtt_client.on_message = on_mqtt_command
        mqtt_client.connect(self.fog_mqtt_host)
        mqtt_client.subscribe('cloud/fog/command', qos=1)
        logger.info("Fog (MQTT): subscribed to cloud/fog/command topic.")

        # Meanwhile, start a separate thread/process to consume model-updates via AMQP:
        self._start_amqp_model_from_cloud()
        mqtt_client.loop_forever()

    def _start_amqp_model_from_cloud(self):
        """
        Consume the cloud’s aggregated-model broadcast over AMQP fanout and
        update the fog model.  This runs in parallel to the MQTT loop.
        """
        connection = self._create_connection()
        channel = connection.channel()
        channel.exchange_declare(exchange='cloud_fog_exchange', exchange_type='fanout', durable=True)

        # create a private queue to get the models from the cloud
        result = channel.queue_declare(queue='', exclusive=True)
        queue_name = result.method.queue
        channel.queue_bind(exchange='cloud_fog_exchange', queue=queue_name)

        def on_amqp_model(ch, method, _props, body):
            msg = json.loads(body)
            if msg.get('model'):
                # same model‐saving logic you already have
                model_bytes = base64.b64decode(msg['model'])
                os.makedirs(os.path.dirname(FogResourcesPaths.FOG_MODEL_FILE_PATH), exist_ok=True)
                with open(FogResourcesPaths.FOG_MODEL_FILE_PATH, 'wb') as f:
                    f.write(model_bytes)
                logger.info("Fog (AMQP): updated fog model from cloud broadcast.")
            ch.basic_ack(delivery_tag=method.delivery_tag)

        channel.basic_consume(queue=queue_name, on_message_callback=on_amqp_model, auto_ack=False)
        # run this consumer in its own thread or process so it doesn’t block the MQTT loop
        threading.Thread(target=channel.start_consuming, daemon=True).start()

    def start_model_listener(self):
        """
        Listen for trained models sent from edges and cache them.
        """
        connection = self._create_connection()
        channel = connection.channel()
        channel.queue_declare(queue='edge_to_fog_models', durable=True)

        def on_edge_model(ch, method, _properties, body):
            payload = json.loads(body)
            edge_mac = payload['edge_mac']
            # Decode and store the model to a temporary path
            model_bytes = base64.b64decode(payload['model'])
            model_path = os.path.join(FogResourcesPaths.MODELS_FOLDER_PATH, f"{edge_mac}_trained_model.keras")
            with open(model_path, "wb") as f:
                f.write(model_bytes)

            metrics = payload['metrics']
            self.edge_models_cache[edge_mac] = {"model_path": model_path, "metrics": metrics}
            logger.info(f"Fog: cached model for edge {edge_mac} with metrics {metrics}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            self.is_ready_to_aggregate()

        channel.basic_consume(
            queue='edge_to_fog_models', on_message_callback=on_edge_model, auto_ack=False
        )
        logger.info("Fog: listening for trained models from edges...")
        channel.start_consuming()

    def is_ready_to_aggregate(self):
        if len(self.edge_models_cache) == len(FederatedNodeState.get_current_node().child_nodes):
            logger.info("Fog has received all edge models and is ready to aggregate them.")
            aggregate_models_with_metrics(self.edge_models_cache)
            logger.info("Fog has succeeded to aggregate edge models. Sending aggregated fog model to cloud.")
            self.send_aggregated_model_to_cloud()

    def send_aggregated_model_to_cloud(self):
        """
        Publish the aggregated fog model to the cloud.
        """
        model_path = FogResourcesPaths.FOG_MODEL_FILE_PATH
        if not os.path.exists(model_path):
            logger.error("Fog: no aggregated model found at %s, cannot send to cloud.", model_path)
            return

        with open(model_path, "rb") as f:
            model_bytes = f.read()
        model_b64 = base64.b64encode(model_bytes).decode('utf-8')

        payload = {
            "fog_name": FederatedNodeState.get_current_node().name,
            "fog_device_mac": FederatedNodeState.get_current_node().device_mac,
            "model": model_b64,
        }

        # Connect to the cloud broker and send the model
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.cloud_amqp_host))
        channel = connection.channel()
        channel.queue_declare(queue="fog_to_cloud_models", durable=True)
        channel.basic_publish(
            exchange='',
            routing_key='fog_to_cloud_models',
            body=json.dumps(payload).encode('utf-8'),
            properties=pika.BasicProperties(delivery_mode=2)  # make message persistent
        )
        connection.close()
        logger.info("Fog: sent aggregated fog model to cloud.")
