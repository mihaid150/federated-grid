import base64
import hashlib
import json
import os
import time
from tempfile import NamedTemporaryFile
from typing import Dict
import socket
import random
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
        self._recent_fog_models = {}
        self.round_id = None

        # ensure status folder exists and try to restore status
        try:
            os.makedirs(CloudResourcesPaths.STATUS_FOLDER_PATH.value, exist_ok=True)
        except Exception as e:
            logger.warning("Cloud: failed to ensure status folder: %s", e)

        self._restore_round_id()

        # cloud-agent specific
        self._global_throttle_rate = None
        self._selected_fog = None

    # ---------------------------
    # status helpers
    # ---------------------------

    def _persist_round_id(self, round_id: int):
        """Atomically persist current round id to disk."""
        path = CloudResourcesPaths.ROUND_FILE_PATH.value
        data = {"round_id": round_id, "ts": int(time.time())}
        try:
            # atomic write: write temp file then replace
            self.round_id = int(round_id)
            with NamedTemporaryFile("w", dir=os.path.dirname(path), delete=False) as tf:
                json.dump(data, tf)
                tf.flush()
                os.fsync(tf.fileno())
                tmp_path = tf.name
            os.replace(tmp_path, path)
            logger.debug("Cloud: persisted round_id=%s to %s", round_id, path)
        except Exception as e:
            logger.warning("Cloud: failed to persist round_id to %s: %s", path, e)

    def _restore_round_id(self):
        """Restore round id from disk, if present."""
        path = CloudResourcesPaths.ROUND_FILE_PATH.value
        try:
            if os.path.exists(path):
                with open(path) as f:
                    data = json.load(f)
                rid = data.get("round_id")
                if rid is not None:
                    self.round_id = rid
                    logger.info("Cloud: restored round_id=%s from %s", rid, path)
        except Exception as e:
            logger.warning("Cloud: failed to restore round_id from %s: %s", path, e)

    # ---------------------------
    # Internal helpers
    # ---------------------------

    def _wait_for_node(self, timeout=300):
        waited = 0
        while FederatedNodeState.get_current_node() is None and waited < timeout:
            logger.info("Cloud: waiting for node initialization...")
            time.sleep(1)
            waited += 1
        return FederatedNodeState.get_current_node()

    def _create_connection(self, retries=10, delay=5) -> pika.BlockingConnection:
        """Helper to create a new RabbitMQ connection with retry logic for cloud node."""
        for attempt in range(1, retries + 1):
            try:
                return pika.BlockingConnection(pika.ConnectionParameters(
                    host=self.cloud_amqp_host,
                    heartbeat=30,
                    blocked_connection_timeout=60,
                    connection_attempts=1,
                    retry_delay=0,
                ))
            except (socket.gaierror, pika.exceptions.AMQPError) as e:
                if attempt == 1:
                    logger.warning("Cloud: AMQP unavailable (%s). Will retry up to %d times...",
                                   e.__class__.__name__, retries)
                if attempt == retries:
                    logger.error("Cloud: could not reach RabbitMQ (%s) after %d attempts.",
                                 self.cloud_amqp_host, retries)
                    raise
                time.sleep(delay + random.uniform(0, 1.0))
                delay = min(delay * 2, 60)

    def _mqtt_publish(self, topic: str, message: dict, qos: int = 1, retain: bool = True,
                      retries=10, delay=5):
        client = mqtt.Client(client_id="cloud-publisher", clean_session=True)
        for attempt in range(1, retries + 1):
            try:
                client.connect(self.cloud_mqtt_host, self.cloud_mqtt_port)
                break
            except Exception as e:
                logger.warning("Cloud MQTT connection failed (attempt %d/%d): %s", attempt, retries, e)
                if attempt == retries:
                    logger.error("Cloud: Could not connect to MQTT broker (%s:%s) after %d retries.",
                                 self.cloud_mqtt_host, self.cloud_mqtt_port, retries)
                    return
                time.sleep(delay)
        client.publish(topic, json.dumps(message), qos=qos, retain=retain)
        client.disconnect()

    def _ensure_per_fog_queues_and_publish(self, channel: pika.adapters.blocking_connection.BlockingChannel,
                                           message_body: bytes):
        """
        Publish to a durable per-fog queue (directly via default exchange).
        This guarantees persistence even if the fog is offline at publish time.
        """
        node = self._wait_for_node()
        fogs = getattr(node, "child_nodes", []) or []

        if not fogs:
            logger.warning("Cloud: no fog nodes registered; nothing to broadcast to.")
            return

        for fog in fogs:
            q = f"cloud_fanout_for_{fog.name}"  # reuse existing fog queue name
            # Durable queue (keep across broker restarts). Optionally make it quorum for more safety:
            # arguments={"x-queue-type": "quorum"}
            channel.queue_declare(queue=q, durable=True, auto_delete=False)
            channel.basic_publish(
                exchange='',              # direct to queue
                routing_key=q,            # per-fog queue
                body=message_body,
                properties=pika.BasicProperties(delivery_mode=2),  # persistent
                mandatory=True,
            )
            logger.info("Cloud (AMQP): enqueued model for fog '%s' in queue '%s'.", fog.name, q)

    # ---------------------------
    # Control-plane (MQTT)
    # ---------------------------

    def notify_all_edges_to_create_local_model(self):
        self._mqtt_publish(topic='cloud/fog/command', message={'command': '0'})
        logger.info("Cloud (MQTT): sent command to fogs instructing edges to create local model.")

    def notify_all_edges_to_start_first_training(self, data: Dict[str, any]):
        round_id = data.get("round_id", int(time.time() * 1000))
        self.round_id = round_id
        self._persist_round_id(round_id)  # <-- NEW
        cmd = {'command': '1', 'cmd_id': int(time.time() * 1000), 'round_id': round_id, 'data': data}
        self._mqtt_publish('cloud/fog/command', cmd, qos=1, retain=False)

        # emit event for the external cloud-agent service
        self._mqtt_publish('cloud/events/round-started',
                           {"round_id": round_id, 'data': data, "ts": int(time.time())}, qos=1, retain=False)

    # ---------------------------
    # Model broadcast (AMQP)
    # ---------------------------

    def broadcast_cloud_model(self, data: Dict[str, any]):
        """
        Broadcast an aggregated cloud model (binary) by publishing directly to
        each per-fog durable queue. This works even if a fog is offline.
        """
        self.fog_models_cache.clear()
        round_id = data.get("round_id")
        self.round_id = round_id
        self._persist_round_id(round_id)

        model_path = CloudResourcesPaths.CLOUD_MODEL_FILE_PATH.value
        if not os.path.exists(model_path):
            logger.error("Cloud: no aggregated model found at %s, cannot broadcast.", model_path)
            return

        with open(model_path, "rb") as f:
            model_b64 = base64.b64encode(f.read()).decode('utf-8')

        message = {"command": "2", "round_id": round_id, "model": model_b64, "data": data}
        message_body = json.dumps(message).encode('utf-8')

        connection = self._create_connection()
        try:
            channel = connection.channel()
            # Publish to each fog queue (create if missing)
            self._ensure_per_fog_queues_and_publish(channel, message_body)
            logger.info("Cloud (AMQP): broadcast cloud model to per-fog queues.")
        finally:
            try:
                connection.close()
            except Exception:
                pass

        # emit event for external cloud-agent service
        try:
            self._mqtt_publish('cloud/events/cloud-model-broadcast',
                               {"round_id": round_id, "ts": int(time.time())}, qos=1, retain=False)
        except Exception as e:
            logger.warning(f"Cloud: failed to publish cloud-model-broadcast message to cloud-agent: {e}")

    # ---------------------------
    # Ingest aggregated fog models (AMQP)
    # ---------------------------

    def start_amqp_listener(self):
        """
        Listen for aggregated models from fogs and cache them.
        Early de-dupe by message_id, then by content hash.
        """
        node = self._wait_for_node()
        if not node:
            logger.error("Cloud: node not initialized; AMQP listener not started.")
            return

        if self.round_id is None:
            # one more attempt to restore in case init happened race-y
            self._restore_round_id()

        os.makedirs(CloudResourcesPaths.MODELS_FOLDER_PATH.value, exist_ok=True)
        connection = self._create_connection()
        channel = connection.channel()
        channel.basic_qos(prefetch_count=1)
        declare_result = channel.queue_declare(queue='fog_to_cloud_models', durable=True)

        if os.getenv("DEV_PURGE_ON_BOOT", "false").lower() == "true":
            channel.queue_purge('fog_to_cloud_models')
            logger.info("Cloud (DEV): purged fog_to_cloud_models on boot.")

        logger.info("Cloud: declared queue 'fog_to_cloud_models' → message_count=%d, consumer_count=%d",
                    declare_result.method.message_count,
                    declare_result.method.consumer_count)

        # message_id -> timestamp (seconds)
        recent_ids = {}
        MSG_TTL = 900  # 15 minutes

        def _purge_old_ids(now):
            to_del = [mid for mid, ts in recent_ids.items() if now - ts > MSG_TTL]
            for mid in to_del:
                recent_ids.pop(mid, None)

        def on_fog_model(ch, method, properties, body):
            now = time.time()
            _purge_old_ids(now)

            msg_id = getattr(properties, "message_id", None)
            if msg_id:
                if msg_id in recent_ids:
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    return
                recent_ids[msg_id] = now

            try:
                payload = json.loads(body)
            except Exception:
                logger.warning("Cloud: non-JSON AMQP payload; acking.")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            fog_device_mac = payload.get('fog_device_mac')
            fog_name = payload.get('fog_name')
            model_b64 = payload.get('model')
            model_hash = payload.get('hash')
            round_id = payload.get('round_id')

            if self.round_id is None:
                logger.warning("Cloud: no active round; discarding fog model.")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            if round_id != self.round_id:
                logger.warning("Cloud: round-id mismatch (cloud=%s, got=%s); ignoring model.",
                               self.round_id, round_id)
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            if not fog_device_mac or not fog_name or not model_b64:
                logger.warning("Cloud: malformed fog message (missing fields); acking.")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            # Fallback dedupe by content hash
            key = f"{fog_device_mac}:{fog_name}"
            if not model_hash:
                try:
                    model_hash = hashlib.sha256(base64.b64decode(model_b64)).hexdigest()
                except Exception:
                    logger.warning("Cloud: invalid base64 model; acking.")
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    return

            prev = self._recent_fog_models.get(key)
            if prev and prev["hash"] == model_hash and (now - prev["ts"] < 900):
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            try:
                model_bytes = base64.b64decode(model_b64)
                model_path = os.path.join(
                    CloudResourcesPaths.MODELS_FOLDER_PATH.value,
                    f'{fog_name}_aggregated_model.keras'
                )
                with open(model_path, 'wb') as f:
                    f.write(model_bytes)
                    f.flush()
                    os.fsync(f.fileno())

                map_id = fog_device_mac + '_' + fog_name
                self.fog_models_cache[map_id] = {"model_path": model_path}
                self._recent_fog_models[key] = {"hash": model_hash, "ts": now}
                logger.info("Cloud: cached aggregated model from fog %s at %s.", fog_name, model_path)

                # emit event for external cloud-agent service
                try:
                    self._mqtt_publish('cloud/events/fog-model-received',
                                       {"round_id": round_id, "fog_name": fog_name, "hash": model_hash,
                                        "ts": int(time.time())}, qos=1, retain=False)
                except Exception as e:
                    logger.warning(f"Cloud: failed to publish fog-model received for {fog_name}: {e}")

            except Exception as e:
                logger.error("Cloud: failed to decode/save model from fog %s: %s", fog_name, e)

            ch.basic_ack(delivery_tag=method.delivery_tag)
            self.is_ready_to_aggregate()

        channel.basic_consume(
            queue='fog_to_cloud_models', on_message_callback=on_fog_model, auto_ack=False
        )
        logger.info("Cloud: listening for aggregated models from fogs...")
        channel.start_consuming()

    # ---------------------------
    # Aggregation gate
    # ---------------------------

    def is_ready_to_aggregate(self):
        if len(self.fog_models_cache) == len(FederatedNodeState.get_current_node().child_nodes):
            logger.info("Cloud has received all fog models and is ready to aggregate them.")
            aggregate_received_models(self.fog_models_cache)
            self.fog_models_cache.clear()
            logger.info("Cloud has succeeded to aggregate fog models and obtained cloud model.")

    # ---------------------------
    # Optional MQTT listener (unchanged)
    # ---------------------------

    def start_mqtt_listener(self):
        """
        Listen for MQTT messages from fogs or edges.
        """
        node = self._wait_for_node()
        if not node:
            logger.error("Cloud: node not initialized; MQTT listener not started.")
            return

        def on_connect(client, userdata, flags, rc):
            if rc == 0:
                logger.info("Cloud MQTT: connected successfully.")
                client.subscribe("fog/cloud/updates")

                # listen for commands coming from external cloud-agent service
                client.subscribe("cloud/agent/commands", qos=1)
            else:
                logger.error("Cloud MQTT: failed to connect, code %s", rc)

        def on_message(client, userdata, msg):
            payload = msg.payload.decode("utf-8")
            topic = msg.topic
            if topic == "cloud/agent/commands":
                try:
                    data = json.loads(payload)
                except Exception:
                    logger.warning(f"Cloud: bad command for payload {payload}")
                    return

                cmd = str(data.get("cmd", "")).upper()

                if cmd == "GLOBAL_THROTTLE":
                    rate = float(data.get("rate", 0))
                    self._global_throttle_rate = rate
                    logger.info(f"Cloud: GLOBAL_THROTTLE rate set to {rate}")

                    # propagate to fogs the status for THROTTLE
                    # TODO: update fogs to understand this command
                    self._mqtt_publish("cloud/fog/command",
                                       {"command": "GLOBAL_THROTTLE", "rate": rate, "ts": int(time.time())}, qos=1, retain=False)
                elif cmd == "SELECT_FOG":
                    self._selected_fog = data.get("target")
                    logger.info(f"Cloud: SELECT_FOG target set to {self._selected_fog}")
                else:
                    logger.info(f"Cloud: unknown command {cmd}")
                return
            # default logging for other topics
            logger.info("Cloud MQTT: received on %s: %s", msg.topic, msg.payload.decode())

        mqtt_client = mqtt.Client()
        mqtt_client.on_connect = on_connect
        mqtt_client.on_message = on_message

        mqtt_client.connect(self.cloud_mqtt_host, self.cloud_mqtt_port)
        mqtt_client.loop_forever()