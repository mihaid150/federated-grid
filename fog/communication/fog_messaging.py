import base64
import json
import os
import threading
import time

import pika
import paho.mqtt.client as mqtt
from shared.logging_config import logger
from shared.node_state import FederatedNodeState
from fog.communication.fog_resources_paths import FogResourcesPaths
from fog.model.model_aggregation_service import aggregate_models_with_metrics


class FogMessaging:
    def __init__(self,
                 fog_amqp_host: str = os.getenv('FOG_RABBITMQ_HOST', 'rabbitmq-fog1'),
                 cloud_amqp_host: str = os.getenv('CLOUD_RABBITMQ_HOST', 'rabbitmq-cloud'),
                 fog_mqtt_host: str = os.getenv('FOG_MQTT_HOST', 'mqtt-fog1'),
                 fog_mqtt_port: int = int(os.getenv('FOG_MQTT_PORT', 1883)),
                 cloud_mqtt_host: str = os.getenv('CLOUD_MQTT_HOST', 'mqtt-cloud'),
                 cloud_mqtt_port: int = int(os.getenv('CLOUD_MQTT_PORT', 1883)),):
        """
        :param fog_amqp_host: hostname or IP of the fog's RabbitMQ broker.
        :param cloud_amqp_host: hostname or IP of the cloud's RabbitMQ broker.
        :param fog_mqtt_host: hostname or IP of the fog's MQTT broker.
        :param fog_mqtt_port: port of the fog's MQTT broker.
        """
        self.fog_amqp_host = fog_amqp_host
        self.cloud_amqp_host = cloud_amqp_host
        self.fog_mqtt_host = fog_mqtt_host
        self.fog_mqtt_port = fog_mqtt_port
        self.cloud_mqtt_host = cloud_mqtt_host
        self.cloud_mqtt_port = cloud_mqtt_port
        # Cache of received models keyed by edge
        self.edge_models_cache = {}

    def _create_connection(self, retries=10, delay=5) -> pika.BlockingConnection:
        """Helper to create a new RabbitMQ connection with retry logic for fog node."""
        for attempt in range(1, retries + 1):
            try:
                connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.fog_amqp_host))
                return connection
            except pika.exceptions.AMQPConnectionError as e:
                logger.warning(f"Fog AMQP connection failed (attempt {attempt}/{retries}): {e})")
                if attempt == retries:
                    logger.error(f"Could not connect to RabbitMQ broker ({self.fog_amqp_host}) after {retries} retries.")
                    raise
                time.sleep(delay)
        return None

    def start_mqtt_listener(self, retries=10, delay=5):
        """
        Subscribe to cloud commands over MQTT and forward them to edges.
        Large model updates from the cloud will still arrive via AMQP and
        are handled by the RabbitMQ consumer in start_model_from_cloud().
        """

        cloud_client = mqtt.Client()
        fog_client = mqtt.Client()

        def connect_with_retry(client, host, port, label):
            for attempt in range(1, retries + 1):
                try:
                    client.connect(host, port)
                    break
                except Exception as e:
                    logger.warning(f"{label} MQTT connect failed (attempt {attempt}/{retries}): {e}")
                    if attempt == retries:
                        logger.error(f"{label}: Could not connect to {host}:{port} after {retries} retries.")
                        raise
                    time.sleep(delay)

        def on_cloud_message(_client, _userdata, msg):
            try:
                payload = json.loads(msg.payload.decode())
            except Exception as e:
                logger.warning(f"Fog: bad MQTT payload from cloud: {e}")
                return

            command = payload.get('command')
            if command is None:
                return

            for edge in FederatedNodeState.get_current_node().child_nodes:
                topic = f'fog/{edge.name}/command'
                fog_client.publish(topic, msg.payload, qos=1)
                logger.info(f"Fog (relay): sent command {command} to edge {edge.name} on {topic}")

        cloud_client.on_message = on_cloud_message

        # Connect both
        connect_with_retry(cloud_client, self.cloud_mqtt_host, self.cloud_mqtt_port, "Cloud")
        connect_with_retry(fog_client, self.fog_mqtt_host, self.fog_mqtt_port, "Fog-local")

        # Subscribe on cloud side; edges subscribe to fog/{edge}/command already
        cloud_client.subscribe('cloud/fog/command', qos=1)
        logger.info("Fog: subscribed to cloud/fog/command on CLOUD broker.")

        # Use non-blocking loops so both clients stay alive in this thread
        cloud_client.loop_start()
        fog_client.loop_start()

        # Keep thread alive
        while True:
            time.sleep(60)

    def start_amqp_listener(self):
        """
        Bridge: consume cloud fanout on the CLOUD broker and forward to edges on the FOG broker.
        - Reconnects automatically on failures
        - Uses a fresh fog-local connection per message (thread-safe)
        """

        def run():
            while True:
                cloud_conn = None
                try:
                    # ----- connect to CLOUD broker (consumer) -----
                    cloud_params = pika.ConnectionParameters(
                        host=self.cloud_amqp_host,
                        heartbeat=30,
                        blocked_connection_timeout=60,
                        client_properties={
                            "connection_name": f"fog:{FederatedNodeState.get_current_node().name}:cloud-consumer"},
                    )
                    cloud_conn = pika.BlockingConnection(cloud_params)
                    cloud_ch = cloud_conn.channel()
                    cloud_ch.exchange_declare(exchange='cloud_fog_exchange', exchange_type='fanout', durable=True)

                    q = cloud_ch.queue_declare(queue='', exclusive=True, auto_delete=True, durable=False)
                    queue_name = q.method.queue
                    cloud_ch.queue_bind(exchange='cloud_fog_exchange', queue=queue_name)
                    cloud_ch.basic_qos(prefetch_count=1)

                    def on_amqp_model(ch, method, _props, body):
                        # decode message
                        try:
                            msg = json.loads(body.decode("utf-8"))
                        except Exception as e:
                            logger.warning(f"Fog: invalid cloud AMQP payload: {e}")
                            ch.basic_ack(delivery_tag=method.delivery_tag)
                            return

                        # save cloud model locally if present
                        if msg.get("model"):
                            try:
                                os.makedirs(os.path.dirname(FogResourcesPaths.FOG_MODEL_FILE_PATH.value), exist_ok=True)
                                with open(FogResourcesPaths.FOG_MODEL_FILE_PATH.value, "wb") as f:
                                    f.write(base64.b64decode(msg["model"]))
                                    f.flush();
                                    os.fsync(f.fileno())
                                logger.info("Fog (AMQP): updated fog model from cloud broadcast.")
                            except Exception as e:
                                logger.exception(f"Fog: failed writing fog model: {e}")

                        # ----- forward to edges on FOG broker (NEW conn per callback) -----
                        try:
                            fog_params = pika.ConnectionParameters(
                                host=self.fog_amqp_host,
                                heartbeat=30,
                                blocked_connection_timeout=60,
                                client_properties={
                                    "connection_name": f"fog:{FederatedNodeState.get_current_node().name}:edge-forwarder"},
                            )
                            with pika.BlockingConnection(fog_params) as fog_conn:
                                fog_ch = fog_conn.channel()
                                # declare + publish for each edge
                                for edge in FederatedNodeState.get_current_node().child_nodes:
                                    queue = f"edge_{edge.name}_messages_queue"
                                    fog_ch.queue_declare(queue=queue, durable=True)
                                    fog_ch.basic_publish(
                                        exchange='',
                                        routing_key=queue,
                                        body=json.dumps(msg).encode('utf-8'),
                                        properties=pika.BasicProperties(
                                            delivery_mode=2,
                                            content_type="application/json",
                                        ),
                                    )
                            logger.info("Fog (AMQP): broadcasted cloud message to edges via fog broker.")
                        except Exception as e:
                            logger.exception(f"Fog: failed forwarding to edges: {e}")

                        ch.basic_ack(delivery_tag=method.delivery_tag)

                    cloud_ch.basic_consume(queue=queue_name, on_message_callback=on_amqp_model, auto_ack=False)
                    logger.info("Fog: consuming cloud broadcast on RabbitMQ (fanout).")
                    cloud_ch.start_consuming()

                except pika.exceptions.AMQPError as e:
                    logger.warning(f"Fog: cloud AMQP consumer disconnected: {e}; retrying in 5s...")
                    time.sleep(5)
                except Exception as e:
                    logger.exception(f"Fog: unexpected error in cloud AMQP bridge: {e}; retrying in 5s...")
                    time.sleep(5)
                finally:
                    try:
                        if cloud_conn and cloud_conn.is_open:
                            cloud_conn.close()
                    except Exception:
                        pass

        threading.Thread(target=run, daemon=True).start()

    def start_edge_model_listener(self):
        """
        Listen for trained models sent from edges and cache them.
        """
        connection = self._create_connection()
        channel = connection.channel()
        channel.queue_declare(queue='edge_to_fog_models', durable=True)

        def on_edge_model(ch, method, _properties, body):
            payload = json.loads(body)
            edge_mac = payload['edge_mac']
            edge_name = payload['edge_name']
            # Decode and store the model to a temporary path
            model_bytes = base64.b64decode(payload['model'])
            model_path = os.path.join(FogResourcesPaths.MODELS_FOLDER_PATH.value, f"{edge_name}_trained_model.keras")
            with open(model_path, "wb") as f:
                f.write(model_bytes)
                f.flush()
                os.fsync(f.fileno())

            metrics = payload['metrics']
            map_id = edge_mac + '_' + edge_name
            self.edge_models_cache[map_id] = {"model_path": model_path, "metrics": metrics}
            logger.info(f"Fog: cached model for edge {edge_name} with metrics {metrics}")
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
            try:
                aggregated = aggregate_models_with_metrics(self.edge_models_cache)
            except Exception as e:
                logger.exception(f"Fog: aggregation failed: {e}")
                return
            if aggregated is None:
                logger.error("Fog: aggregation produced no model (skipping send to cloud).")
                return
            logger.info("Fog has succeeded to aggregate edge models. Sending aggregated fog model to cloud.")
            self.send_aggregated_model_to_cloud()
            self.edge_models_cache.clear()


    def send_aggregated_model_to_cloud(self):
        """
        Publish the aggregated fog model to the cloud.
        """

        connection = self._create_connection_to_cloud()

        model_path = FogResourcesPaths.FOG_MODEL_FILE_PATH.value
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

    def _create_connection_to_cloud(self, retries=10, delay=5):
        for attempt in range(1, retries + 1):
            try:
                connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.cloud_amqp_host))
                return connection
            except pika.exceptions.AMQPConnectionError as e:
                logger.warning(f"Cloud AMQP connection failed (attempt {attempt}/{retries}): {e}")
                if attempt == retries:
                    logger.error(
                        f"Could not connect to cloud RabbitMQ ({self.cloud_amqp_host}) after {retries} attempts.")
                    raise
                time.sleep(delay)
        return None
