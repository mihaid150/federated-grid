import base64
import hashlib
import json
import os
import threading
import time
import random
import socket

import pika
import paho.mqtt.client as mqtt
from pika import exceptions as px

from shared.logging_config import logger
from shared.node_state import FederatedNodeState
from fog.communication.fog_resources_paths import FogResourcesPaths
from fog.model.model_aggregation_service import aggregate_models_with_metrics
from shared.utils import delete_files_containing

def _purge_outbox_all():
    try:
        delete_files_containing(
            FogResourcesPaths.OUTBOX_FOLDER_PATH.value, None, [".json", ".keras"]
        )
        logger.info("Fog: purged outbox (all files).")
    except Exception as e:
        logger.warning("Fog: purge outbox failed: %s", e)

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
        self._last_cmd_id = None
        self._outbox_enabled = os.getenv('FOG_RESUME_OUTBOX_ON_BOOT', 'false').lower() == 'true'
        self.current_round = None
        _purge_outbox_all()
        logger.info("Fog: purged outbox on boot (no active round).")
        self.OUTBOX_TTL_SECS = int(os.getenv('FOG_OUTBOX_TTL_SECS', '86400'))  # 24h default

        # ---- topics to integrate with fog-agent service ----
        self.topic_edge_model_received = os.getenv("TOPIC_EDGE_MODEL_RECEIVED", "fog/events/edge-model-received")
        self.topic_aggregation_complete = os.getenv("TOPIC_AGGREGATION_COMPLETE", "fog/events/aggregation-complete")
        self.topic_cloud_model_downlink = os.getenv("TOPIC_CLOUD_MODEL_DOWNLINK", "fog/events/cloud-model-downlink")
        self.topic_agent_commands = os.getenv("TOPIC_AGENT_COMMANDS", "fog/agent/commands")

        if os.getenv("FOG_PURGE_OUTBOX_ON_BOOT", "false").lower() == "true" and self.current_round is None:
            delete_files_containing(FogResourcesPaths.OUTBOX_FOLDER_PATH.value, None, [".json", ".keras"])
            logger.info("Fog (DEV): purged outbox on boot (no active round).")

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

    def _prune_outbox_for_round(self):
        outbox_dir = FogResourcesPaths.OUTBOX_FOLDER_PATH.value
        try:
            for fname in os.listdir(outbox_dir):
                if not fname.endswith(".json"):
                    continue
                meta_path = os.path.join(outbox_dir, fname)
                try:
                    with open(meta_path) as f:
                        meta = json.load(f)
                except Exception:
                    try:
                        os.remove(meta_path)
                    except:
                        pass
                    continue
                if meta.get("round_id") != self.current_round:
                    # remove non-matching round files
                    try:
                        os.remove(meta_path)
                    except:
                        pass
                    try:
                        os.remove(meta.get("model_file", ""))
                    except:
                        pass
        except FileNotFoundError:
            pass

    def start_mqtt_listener(self):
        """
        Robust MQTT bridge:
          - Persistent sessions (clean_session=False)
          - LWTs set before connecting
          - connect_async + loop_start (non-blocking, no crash if broker is down)
          - Automatic reconnect with backoff
          - Watchdog to re-issue connect_async after DNS failures
          - Relays cloud 'cloud/fog/command' → per-edge retained topics
        """
        CLEAR_AFTER_SECONDS = 300
        WATCHDOG_PERIOD = int(os.getenv("FOG_MQTT_WATCHDOG_SECS", "10"))

        fog_name = FederatedNodeState.get_current_node().name
        cloud_host, cloud_port = self.cloud_mqtt_host, self.cloud_mqtt_port
        fog_local_host, fog_local_port = self.fog_mqtt_host, self.fog_mqtt_port

        cloud_client = mqtt.Client(client_id=f"fog-cloud-{fog_name}", clean_session=False)
        fog_client = mqtt.Client(client_id=f"fog-local-{fog_name}", clean_session=False)

        # LWT must be set before connect
        cloud_client.will_set(f"fogs/{fog_name}/status", payload="offline", qos=1, retain=True)
        fog_client.will_set(f"fog/{fog_name}/local_status", payload="offline", qos=1, retain=True)

        # Exponential reconnect (let paho manage it)
        cloud_client.reconnect_delay_set(min_delay=1, max_delay=60)
        fog_client.reconnect_delay_set(min_delay=1, max_delay=60)

        # Track per-topic clear timers so a new command cancels the previous clear
        clear_timers = {}

        def _mark_online(client, topic):
            try:
                client.publish(topic, payload="online", qos=1, retain=True)
            except Exception as e:
                logger.warning("MQTT: failed to publish online status to %s: %s", topic, e)

        # -------------------------
        # Cloud client callbacks
        # -------------------------
        def on_cloud_connect(client, _userdata, _flags, rc):
            if rc == 0:
                _mark_online(client, f"fogs/{fog_name}/status")
                # (Re)subscribe after reconnects as well
                try:
                    client.subscribe("cloud/fog/command", qos=1)
                    logger.info("Fog: subscribed to cloud/fog/command on CLOUD broker.")
                except Exception as e:
                    logger.warning("Fog: subscribe failed on CLOUD broker: %s", e)
            else:
                logger.error("Fog (cloud client): connect failed, rc=%s", rc)

        def on_cloud_disconnect(_client, _userdata, rc):
            # rc==0 means a planned disconnect; otherwise broker/network issue.
            if rc != 0:
                logger.warning("Fog: disconnected from CLOUD MQTT (rc=%s). Will auto-reconnect.", rc)

        def on_cloud_message(_client, _userdata, msg):
            try:
                payload = json.loads(msg.payload.decode())
            except Exception as e:
                logger.warning("Fog: bad MQTT payload from cloud: %s", e)
                return

            command = payload.get("command")
            if command is None:
                return

            # Pick up round_id when present
            round_id = payload.get("round_id")
            if round_id is not None and round_id != self.current_round:
                logger.info("Fog: new round_id=%s (was %s) → purging outbox.", round_id, self.current_round)
                self.current_round = round_id
                _purge_outbox_all()

            # Turn on uplink only for real rounds (training/broadcast)
            if command in ('1', '2'):
                if not self._outbox_enabled:
                    logger.info("Fog: enabling outbox worker (command=%s).", command)
                self._outbox_enabled = True

            cmd_id = payload.get("cmd_id")
            if cmd_id is not None and cmd_id == self._last_cmd_id:
                logger.info("Fog: duplicate cmd_id %s from cloud ignored.", cmd_id)
                return
            self._last_cmd_id = cmd_id

            # Relay to per-edge topics with retain so late/reconnecting edges get the latest command
            edges = getattr(FederatedNodeState.get_current_node(), "child_nodes", []) or []
            for edge in edges:
                topic = f"fog/{edge.name}/command"
                try:
                    fog_client.publish(topic, json.dumps(payload), qos=1, retain=True)
                    logger.info("Fog (relay): sent command %s to edge %s on %s (retained)", command, edge.name, topic)
                    _schedule_clear(topic)
                except Exception as e:
                    logger.warning("Fog: failed to publish command to %s: %s", topic, e)

        cloud_client.on_connect = on_cloud_connect
        cloud_client.on_disconnect = on_cloud_disconnect
        cloud_client.on_message = on_cloud_message

        # -------------------------
        # Local fog broker client
        # -------------------------
        def on_fog_connect(client, _userdata, _flags, rc):
            if rc == 0:
                _mark_online(client, f"fog/{fog_name}/local_status")
                try:
                    client.subscribe(self.topic_agent_commands, qos=1)
                    logger.info("Fog: subscribed to %s on LOCAL broker.", self.topic_agent_commands)
                except Exception as e:
                    logger.warning("Fog: subscribe failed on LOCAL broker: %s", e)
            else:
                logger.error("Fog (local client): connect failed, rc=%s", rc)

        def on_fog_disconnect(_client, _userdata, rc):
            if rc != 0:
                logger.warning("Fog: disconnected from LOCAL MQTT (rc=%s). Will auto-reconnect.", rc)

        def on_fog_local_message(_client, _userdata, msg):
            # Commands issued by fog-agent service
            try:
                payload = json.loads(msg.payload.decode())
            except Exception as e:
                logger.warning("Fog: bad MQTT payload on %s: %s", msg.topic, e)
                return

            cmd = payload.get("cmd")
            if not cmd:
                return

            edges = getattr(FederatedNodeState.get_current_node(), "child_nodes", []) or []

            # Basic command mapping — adjust as your contract evolves
            if cmd == "THROTTLE":
                # Relay same payload to all edges
                for edge in edges:
                    topic = f"fog/{edge.name}/command"
                    try:
                        fog_client.publish(topic, json.dumps(payload), qos=1, retain=False)
                        logger.info("Fog (agent→edge): relayed THROTTLE to %s", topic)
                    except Exception as e:
                        logger.warning("Fog: failed to relay THROTTLE to %s: %s", topic, e)

            elif cmd == "RETRAIN_FOG":
                # Ask edges to retrain locally (re-using the same per-edge command topic)
                retrain = {
                    "command": "REQUEST_RETRAIN",
                    "reason": payload.get("reason", "policy"),
                    "params": payload.get("params", {"window_days": 2}),
                    "ts": int(time.time())
                }
                for edge in edges:
                    topic = f"fog/{edge.name}/command"
                    try:
                        fog_client.publish(topic, json.dumps(retrain), qos=1, retain=False)
                        logger.info("Fog (agent→edge): relayed RETRAIN to %s", topic)
                    except Exception as e:
                        logger.warning("Fog: failed to relay RETRAIN to %s: %s", topic, e)

            elif cmd == "SUGGEST_MODEL":
                # Targeted hint to one edge
                target = payload.get("target")
                if target:
                    topic = f"fog/{target}/command"
                    try:
                        fog_client.publish(topic, json.dumps(payload), qos=1, retain=False)
                        logger.info("Fog (agent→edge): relayed SUGGEST_MODEL to %s", topic)
                    except Exception as e:
                        logger.warning("Fog: failed to relay SUGGEST_MODEL to %s: %s", topic, e)
                else:
                    logger.info("Fog: SUGGEST_MODEL received without 'target'; ignoring.")
            else:
                logger.info("Fog: unknown agent command '%s' ignored.", cmd)

        fog_client.on_connect = on_fog_connect
        fog_client.on_disconnect = on_fog_disconnect
        fog_client.on_message = on_fog_local_message

        # -------------------------
        # Per-edge retained clear
        # -------------------------
        def _schedule_clear(topic):
            t = clear_timers.pop(topic, None)
            if t and t.is_alive():
                t.cancel()

            def _clear():
                try:
                    fog_client.publish(topic, payload=b"", qos=1, retain=True)
                    logger.info("Fog: cleared retained command on %s", topic)
                except Exception as e:
                    logger.warning("Fog: failed to clear retained command on %s: %s", topic, e)
                finally:
                    clear_timers.pop(topic, None)

            timer = threading.Timer(CLEAR_AFTER_SECONDS, _clear)
            clear_timers[topic] = timer
            timer.start()

        # -------------------------
        # Start both clients (async)
        # -------------------------
        cloud_client.loop_start()
        fog_client.loop_start()

        # Use async connect so failures don’t throw or kill the thread
        try:
            cloud_client.connect_async(cloud_host, cloud_port, keepalive=60)
        except Exception as e:
            # connect_async normally shouldn’t raise, but guard anyway
            logger.warning("Fog: initial connect_async to CLOUD MQTT failed: %s", e)

        try:
            fog_client.connect_async(fog_local_host, fog_local_port, keepalive=60)
        except Exception as e:
            logger.warning("Fog: initial connect_async to LOCAL MQTT failed: %s", e)

        # -------------------------
        # Watchdog: handle DNS flaps / stuck clients
        # -------------------------
        def _watchdog():
            # If cloud DNS is not ready yet, the client may be stuck. Nudge it periodically.
            while True:
                try:
                    # When not connected and not connecting, poke connect_async again
                    if not cloud_client.is_connected():
                        # Optional: try to resolve to give early visibility in logs
                        try:
                            socket.getaddrinfo(cloud_host, cloud_port, 0, socket.SOCK_STREAM)
                        except Exception as dns_e:
                            logger.debug("Fog: CLOUD MQTT DNS still not resolvable (%s:%s): %s",
                                         cloud_host, cloud_port, dns_e)
                        try:
                            cloud_client.connect_async(cloud_host, cloud_port, keepalive=60)
                        except Exception as e:
                            logger.debug("Fog: watchdog connect_async nudge failed: %s", e)
                    # Local broker should be up; still keep it resilient.
                    if not fog_client.is_connected():
                        try:
                            fog_client.connect_async(fog_local_host, fog_local_port, keepalive=60)
                        except Exception as e:
                            logger.debug("Fog: local watchdog connect_async nudge failed: %s", e)
                except Exception:
                    # Never let watchdog die
                    logger.exception("Fog: MQTT watchdog exception (ignored).")
                time.sleep(WATCHDOG_PERIOD)

        threading.Thread(target=_watchdog, daemon=True).start()

        # Keep thread alive without blocking paho’s loop threads
        while True:
            time.sleep(60)

            time.sleep(60)

    def start_amqp_listener(self):
        """
        Bridge: consume the cloud's per-fog durable queue on the CLOUD broker
        and forward messages to edges on the FOG broker.

        This version DOES NOT use a fanout exchange. The cloud publishes directly
        to the per-fog queue (e.g., 'cloud_fanout_for_fog1'), which persists even
        if this fog is offline at publish time.
        """

        def run():
            fog_name = FederatedNodeState.get_current_node().name
            delay = 5
            max_delay = 60
            announced_down = False
            next_warn_at = 0.0

            # Optional: use quorum queues for stronger durability
            use_quorum = os.getenv("FOG_USE_QUORUM_QUEUES", "false").lower() == "true"
            queue_args = {"x-queue-type": "quorum"} if use_quorum else None

            while True:
                cloud_conn = None
                try:
                    cloud_params = pika.ConnectionParameters(
                        host=self.cloud_amqp_host,
                        heartbeat=30,
                        blocked_connection_timeout=60,
                        connection_attempts=1,
                        retry_delay=0,
                        client_properties={"connection_name": f"fog:{fog_name}:cloud-consumer"},
                    )
                    cloud_conn = pika.BlockingConnection(cloud_params)
                    cloud_ch = cloud_conn.channel()

                    queue_name = f'cloud_fanout_for_{fog_name}'
                    declare_result = cloud_ch.queue_declare(
                        queue=queue_name,
                        durable=True,
                        auto_delete=False,
                        arguments=queue_args
                    )

                    logger.info(
                        "Fog: declared per-fog queue '%s' → message_count=%d, consumer_count=%d (quorum=%s)",
                        declare_result.method.queue,
                        getattr(declare_result.method, "message_count", -1),
                        getattr(declare_result.method, "consumer_count", -1),
                        use_quorum,
                    )

                    cloud_ch.basic_qos(prefetch_count=1)

                    def on_amqp_model(ch, method, _props, body):
                        try:
                            msg = json.loads(body.decode("utf-8"))
                        except Exception as e:
                            logger.warning("Fog: invalid cloud AMQP payload: %s", e)
                            ch.basic_ack(delivery_tag=method.delivery_tag)
                            return

                        # Enable uplink when a real round/broadcast arrives
                        if msg.get("command") == "2" or "model" in msg:
                            if not self._outbox_enabled:
                                logger.info("Fog: enabling outbox worker due to cloud model broadcast.")
                            self._outbox_enabled = True

                        # Save cloud model if present
                        if msg.get("model"):
                            try:
                                os.makedirs(os.path.dirname(FogResourcesPaths.FOG_MODEL_FILE_PATH.value), exist_ok=True)
                                with open(FogResourcesPaths.FOG_MODEL_FILE_PATH.value, "wb") as f:
                                    f.write(base64.b64decode(msg["model"]))
                                    f.flush();
                                    os.fsync(f.fileno())
                                logger.info("Fog (AMQP): updated fog model from cloud broadcast.")

                                # Notify fog-agent about downlink
                                self._publish_fog_event(
                                    self.topic_cloud_model_downlink,
                                    {
                                        "round_id": msg.get("round_id"),
                                        "ts": int(time.time())
                                    }
                                )

                            except Exception as e:
                                logger.exception("Fog: failed writing fog model: %s", e)

                        # Track round and prune old outbox if round changes
                        try:
                            rid = msg.get("round_id")
                            if rid is not None and rid != self.current_round:
                                logger.info("Fog: new round_id=%s (was %s) → purging outbox.", rid, self.current_round)
                                self.current_round = rid
                                _purge_outbox_all()

                        except Exception as e:
                            logger.warning("Fog: failed to process round_id from cloud message: %s", e)

                        # Forward message to local edges via fog broker (per-edge durable queues)
                        try:
                            fog_params = pika.ConnectionParameters(
                                host=self.fog_amqp_host,
                                heartbeat=30,
                                blocked_connection_timeout=60,
                                client_properties={"connection_name": f"fog:{fog_name}:edge-forwarder"},
                            )
                            with pika.BlockingConnection(fog_params) as fog_conn:
                                fog_ch = fog_conn.channel()
                                for edge in getattr(FederatedNodeState.get_current_node(), "child_nodes", []) or []:
                                    edge_q = f"edge_{edge.name}_messages_queue"
                                    fog_ch.queue_declare(queue=edge_q, durable=True, auto_delete=False)
                                    fog_ch.basic_publish(
                                        exchange='',
                                        routing_key=edge_q,
                                        body=json.dumps(msg).encode('utf-8'),
                                        properties=pika.BasicProperties(
                                            delivery_mode=2,
                                            content_type="application/json",
                                        ),
                                    )
                            logger.info("Fog (AMQP): forwarded cloud message to local edges.")
                        except Exception as e:
                            logger.exception("Fog: failed forwarding to edges: %s", e)

                        ch.basic_ack(delivery_tag=method.delivery_tag)

                    cloud_ch.basic_consume(queue=queue_name, on_message_callback=on_amqp_model, auto_ack=False)
                    logger.info("Fog: consuming cloud messages from per-fog queue '%s'.", queue_name)

                    if announced_down:
                        logger.info("Fog: cloud AMQP back online; bridge reconnected.")
                        announced_down = False
                        delay = 5  # reset backoff

                    cloud_ch.start_consuming()

                except (socket.gaierror, pika.exceptions.AMQPError) as e:
                    now = time.time()
                    if not announced_down:
                        logger.warning(
                            "Fog: cloud AMQP unavailable (%s). Retrying with backoff up to %ss...",
                            e.__class__.__name__, max_delay
                        )
                        announced_down = True
                        next_warn_at = now + 30
                    elif now >= next_warn_at:
                        logger.warning(
                            "Fog: still unable to reach cloud AMQP (%s). Next retry in ~%ss (max %ss).",
                            e.__class__.__name__, delay, max_delay
                        )
                        next_warn_at = now + 30
                    time.sleep(delay + random.uniform(0, 1.0))
                    delay = min(delay * 2, max_delay)
                except Exception:
                    logger.exception("Fog: unexpected error in cloud AMQP bridge; retrying in %ss...", delay)
                    time.sleep(delay)
                    delay = min(delay * 2, max_delay)
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
        Survives RabbitMQ restarts and network blips.
        """

        def run():
            delay = 5
            max_delay = 60
            announced_down = False  # track outage state

            while True:
                cloud_conn = None
                try:
                    fog_name = FederatedNodeState.get_current_node().name
                    params = pika.ConnectionParameters(
                        host=self.fog_amqp_host,
                        heartbeat=30,
                        blocked_connection_timeout=60,
                        client_properties={"connection_name": f"fog:{fog_name}:cloud-consumer"},
                    )
                    cloud_conn = pika.BlockingConnection(params)
                    ch = cloud_conn.channel()

                    # Durable queue so messages persist while fog app is down
                    ch.queue_declare(queue='edge_to_fog_models', durable=True, auto_delete=False)
                    ch.basic_qos(prefetch_count=1)

                    def on_edge_model(ch, method, _props, body):
                        try:
                            payload = json.loads(body)
                            edge_mac = payload['edge_mac']
                            edge_name = payload['edge_name']
                            model_bytes = base64.b64decode(payload['model'])
                            model_path = os.path.join(FogResourcesPaths.MODELS_FOLDER_PATH.value,
                                                      f"{edge_name}_trained_model.keras")
                            os.makedirs(os.path.dirname(model_path), exist_ok=True)
                            with open(model_path, "wb") as f:
                                f.write(model_bytes);
                                f.flush();
                                os.fsync(f.fileno())
                            metrics = payload['metrics']
                            map_id = edge_mac + '_' + edge_name
                            self.edge_models_cache[map_id] = {"model_path": model_path, "metrics": metrics}
                            logger.info(f"Fog: cached model for edge {edge_name} with metrics {metrics}")

                            self._publish_fog_event(
                                self.topic_edge_model_received,
                                {
                                    "round_id": self.current_round,
                                    "edge_name": edge_name,
                                    "model_path": model_path,
                                    "metrics": metrics,
                                    "ts": int(time.time())
                                }
                            )

                        except Exception as e:
                            logger.exception("Fog: failed handling edge model: %s", e)
                        finally:
                            ch.basic_ack(delivery_tag=method.delivery_tag)

                            if not self._outbox_enabled:
                                logger.info("Fog: enabling outbox worker (received edge model).")
                                self._outbox_enabled = True

                            self.is_ready_to_aggregate()

                    ch.basic_consume(queue='edge_to_fog_models', on_message_callback=on_edge_model, auto_ack=False)
                    logger.info("Fog: listening for trained models from edges...")

                    if announced_down:
                        logger.info("Fog: cloud AMQP back online; bridge reconnected.")
                        announced_down = False
                    delay = 5  # reset backoff after success
                    ch.start_consuming()

                except (socket.gaierror, pika.exceptions.AMQPError) as e:
                    if not announced_down:
                        # First time we notice the outage: log once (no traceback)
                        logger.warning(
                            "Fog: cloud AMQP unavailable (%s). Will retry with backoff up to %ss...",
                            e.__class__.__name__, max_delay
                        )
                        announced_down = True
                    # Quiet retry logs (no traceback, rate-limited)
                    logger.debug("Fog: retrying AMQP connect in %ss...", delay)
                    time.sleep(delay + random.uniform(0, 1.0))  # add jitter
                    delay = min(delay * 2, max_delay)

                except Exception as e:
                    # Truly unexpected: keep a traceback but only once per loop
                    logger.exception("Fog: unexpected error in cloud AMQP bridge; retrying in %ss...", delay)
                    time.sleep(delay)
                    delay = min(delay * 2, max_delay)

                finally:
                    try:
                        if cloud_conn and cloud_conn.is_open:
                            cloud_conn.close()
                    except Exception:
                        pass
        threading.Thread(target=run, daemon=True).start()

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

            # Notify fog-agent
            self._publish_fog_event(
                self.topic_aggregation_complete,
                {
                    "round_id": self.current_round,
                    "model_path": FogResourcesPaths.FOG_MODEL_FILE_PATH.value,
                    "ts": int(time.time())
                }
            )

            self.send_aggregated_model_to_cloud()
            self.edge_models_cache.clear()

    def send_aggregated_model_to_cloud(self):
        if self.current_round is None:
            logger.info("Fog: no active round; dropping immediate send and keeping nothing (outbox already purged).")
            try:
                os.remove(FogResourcesPaths.FOG_MODEL_FILE_PATH.value)
            except Exception:
                pass
            return

        model_path = FogResourcesPaths.FOG_MODEL_FILE_PATH.value
        with open(model_path, "rb") as f:
            model_bytes = f.read()

        # Always enqueue; worker handles publish/confirm/retry
        self.enqueue_model_for_cloud(model_path, model_bytes=model_bytes)
        try:
            os.remove(model_path)
        except Exception:
            pass

        logger.info("Fog: queued aggregated fog model for cloud uplink (worker will publish).")

    def _create_connection_to_cloud(self):
        delay, max_delay = 5, 60
        while True:
            try:
                return pika.BlockingConnection(pika.ConnectionParameters(
                    host=self.cloud_amqp_host,
                    heartbeat=30,
                    blocked_connection_timeout=60,
                    connection_attempts=1,
                    retry_delay=0,
                ))
            except (socket.gaierror, pika.exceptions.AMQPError) as e:
                # Quiet backoff; a periodic warning elsewhere is fine
                time.sleep(delay + random.uniform(0, 1.0))
                delay = min(delay * 2, max_delay)

    def enqueue_model_for_cloud(self, model_path: str, precomputed_hash: str = None, model_bytes: bytes = None):
        """
        Snapshot the model into the outbox with a hash filename to guarantee idempotency.
        Skip if the same hash is already enqueued. Write meta atomically.
        """
        outbox_dir = FogResourcesPaths.OUTBOX_FOLDER_PATH.value
        os.makedirs(outbox_dir, exist_ok=True)

        if model_bytes is None:
            with open(model_path, "rb") as f:
                model_bytes = f.read()
        model_hash = precomputed_hash or hashlib.sha256(model_bytes).hexdigest()

        meta_path = os.path.join(outbox_dir, f"{model_hash}.json")
        blob_path = os.path.join(outbox_dir, f"{model_hash}.keras")

        if os.path.exists(meta_path) and os.path.exists(blob_path):
            logger.info(f"Fog: model {model_hash} already queued; skipping duplicate.")
            return

        # Write blob first
        with open(blob_path, "wb") as mf:
            mf.write(model_bytes)
            mf.flush();
            os.fsync(mf.fileno())

        fog_name = FederatedNodeState.get_current_node().name
        fog_mac = FederatedNodeState.get_current_node().device_mac

        if self.current_round is None:
            logger.info("Fog: no active round; refusing to enqueue model.")
            return

        meta = {
            "fog_name": fog_name,
            "fog_device_mac": fog_mac,
            "model_file": blob_path,
            "hash": model_hash,
            "message_id": f"{fog_mac}:{fog_name}:{model_hash}",
            "round_id": self.current_round,  # <<<< add this
            "ts": int(time.time()),  # for TTL pruning
        }

        # Atomic write for meta to avoid partial JSON
        tmp_meta = meta_path + ".tmp"
        with open(tmp_meta, "w") as jf:
            json.dump(meta, jf)
            jf.flush();
            os.fsync(jf.fileno())
        os.replace(tmp_meta, meta_path)

        logger.info(f"Fog: queued model {model_hash} for later uplink.")

    def start_cloud_uplink_worker(self):
        def run():
            delay, max_delay = 5, 60
            announced_down = False
            next_warn_at = 0.0

            while True:
                # Gate on round and uploader
                if self.current_round is None:
                    time.sleep(2)
                    continue
                if not self._outbox_enabled:
                    time.sleep(2)
                    continue

                outbox_dir = FogResourcesPaths.OUTBOX_FOLDER_PATH.value
                os.makedirs(outbox_dir, exist_ok=True)

                files = sorted(f for f in os.listdir(outbox_dir) if f.endswith(".json"))
                if not files:
                    time.sleep(2)
                    continue

                cloud_conn = None
                try:
                    cloud_conn = self._create_connection_to_cloud()
                    ch = cloud_conn.channel()
                    ch.confirm_delivery()  # enable sync publisher confirms
                    ch.queue_declare(queue="fog_to_cloud_models", durable=True)

                    # Optional: log unroutable returns if you keep mandatory=True
                    def _on_return(_ch, method, props, body):
                        logger.warning("Fog: broker returned message (reply_code=%s, reply_text=%s, rk=%s).",
                                       getattr(method, "reply_code", "?"),
                                       getattr(method, "reply_text", "?"),
                                       getattr(method, "routing_key", "?"))

                    ch.add_on_return_callback(_on_return)

                    for fname in files:
                        meta_path = os.path.join(outbox_dir, fname)
                        try:
                            with open(meta_path) as f:
                                meta = json.load(f)
                        except Exception as e:
                            logger.warning("Fog: bad outbox meta %s: %s; removing.", meta_path, e)
                            try:
                                os.remove(meta_path)
                            except:
                                pass
                            continue

                        round_id = meta.get("round_id")
                        if round_id != self.current_round:
                            logger.debug("Fog: skip %s (meta round=%s, current=%s)", meta_path, round_id,
                                         self.current_round)
                            continue

                        ts = meta.get("ts", 0)
                        blob_path = meta.get("model_file") or meta.get("model_path")
                        fog_name = meta.get("fog_name")
                        fog_mac = meta.get("fog_device_mac")
                        model_hash = meta.get("hash")
                        msg_id = meta.get("message_id", f"{fog_mac}:{fog_name}:{model_hash}")

                        # TTL prune
                        if self.OUTBOX_TTL_SECS > 0 and (time.time() - ts) > self.OUTBOX_TTL_SECS:
                            logger.info("Fog: pruning expired outbox item %s.", meta_path)
                            try:
                                os.remove(meta_path)
                            except:
                                pass
                            try:
                                os.remove(blob_path or "")
                            except:
                                pass
                            continue

                        if not blob_path or not os.path.exists(blob_path):
                            logger.warning("Fog: missing blob for %s; dropping meta.", meta_path)
                            try:
                                os.remove(meta_path)
                            except:
                                pass
                            continue

                        with open(blob_path, "rb") as mf:
                            model_bytes = mf.read()

                        body = json.dumps({
                            "fog_name": fog_name,
                            "fog_device_mac": fog_mac,
                            "model": base64.b64encode(model_bytes).decode("utf-8"),
                            "hash": model_hash,
                            "round_id": round_id,
                        }).encode("utf-8")

                        props = pika.BasicProperties(
                            delivery_mode=2,
                            content_type="application/json",
                            message_id=msg_id,
                        )

                        ch.confirm_delivery()  # enable publisher confirms once per channel

                        try:
                            ch.basic_publish(
                                exchange="",
                                routing_key="fog_to_cloud_models",
                                body=body,
                                properties=props,
                                mandatory=True,
                            )
                            # If we got here, the broker ACKed the publish
                            logger.info("Fog: publish confirmed by broker (msg_id=%s, hash=%s).", msg_id, model_hash)

                            # safe to delete
                            try:
                                os.remove(meta_path)
                            except:
                                pass
                            try:
                                os.remove(blob_path)
                            except:
                                pass

                        except px.UnroutableError:
                            logger.warning("Fog: unroutable publish (queue missing/bad routing key). Will retry.")
                        except px.NackError:
                            logger.warning("Fog: broker NACKed publish. Will retry.")
                        except px.StreamLostError:
                            logger.warning("Fog: stream lost during publish. Will retry.")


                except (px.NackError, px.UnroutableError, px.StreamLostError) as e:
                    now = time.time()
                    if not announced_down:
                        logger.warning("Fog: publish confirmation failed (%s). Retrying with backoff up to %ss...",
                                       e.__class__.__name__, max_delay)
                        announced_down = True
                        next_warn_at = now + 30
                    elif now >= next_warn_at:
                        logger.warning("Fog: still awaiting publish confirm (%s). Next retry in ~%ss (max %ss).",
                                       e.__class__.__name__, delay, max_delay)
                        next_warn_at = now + 30
                    time.sleep(delay + random.uniform(0, 1.0))
                    delay = min(delay * 2, max_delay)

                except (socket.gaierror, pika.exceptions.AMQPError) as e:
                    now = time.time()
                    if not announced_down:
                        logger.warning("Fog: cloud uplink unavailable (%s). Backing off up to %ss...",
                                       e.__class__.__name__, max_delay)
                        announced_down = True
                        next_warn_at = now + 30
                    elif now >= next_warn_at:
                        logger.warning("Fog: still no cloud uplink (%s). Next retry in ~%ss (max %ss).",
                                       e.__class__.__name__, delay, max_delay)
                        next_warn_at = now + 30
                    time.sleep(delay + random.uniform(0, 1.0))
                    delay = min(delay * 2, max_delay)

                except Exception:
                    logger.exception("Fog: unexpected error in cloud uplink worker; retrying in %ss...", delay)
                    time.sleep(delay)
                    delay = min(delay * 2, max_delay)

                finally:
                    try:
                        if cloud_conn and cloud_conn.is_open:
                            cloud_conn.close()
                    except Exception:
                        pass

        threading.Thread(target=run, daemon=True).start()

    def _publish_fog_event(self, topic: str, payload: dict, qos: int = 1, retain: bool = False):
        try:
            cli = mqtt.Client(client_id=f"fog-events-{int(time.time()*1000)}", clean_session=True)
            cli.connect(self.fog_mqtt_host, self.fog_mqtt_port)
            cli.publish(topic, json.dumps(payload), qos=qos, retain=retain)
            cli.disconnect()
            logger.info("Fog (event): published to %s → %s", topic, payload)
        except Exception as e:
            logger.warning("Fog (event): failed to publish to %s: %s", topic, e)




