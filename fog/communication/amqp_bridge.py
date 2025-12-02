import json, os, time, random, socket
import pika
from pika import exceptions as px
from shared.logging_config import logger
from shared.node_state import FederatedNodeState
from fog.communication.config import FogConfig
from fog.communication.state import FogRoundState
from fog.communication.fog_resources_paths import FogResourcesPaths

class CloudToEdgesBridge:
    def __init__(self, cfg: FogConfig, state: FogRoundState, event_bus):
        self.cfg, self.state, self.event_bus = cfg, state, event_bus

    def start(self):
        cfg, st = self.cfg, self.state
        fog_name = FederatedNodeState.get_current_node().name
        delay, max_delay = 5, 60
        announced_down = False
        queue_args = {"x-queue-type": "quorum"} if cfg.use_quorum_queues else None

        while True:
            conn = None
            try:
                params = pika.ConnectionParameters(
                    host=cfg.cloud_amqp_host, heartbeat=30, blocked_connection_timeout=60,
                    connection_attempts=1, retry_delay=0,
                    client_properties={"connection_name": f"fog:{fog_name}:cloud-consumer"},
                )
                conn = pika.BlockingConnection(params)
                ch = conn.channel()

                qname = f"cloud_fanout_for_{fog_name}"
                declare_result = ch.queue_declare(queue=qname, durable=True, auto_delete=False, arguments=queue_args)
                if cfg.purge_amqp_on_boot:
                    try:
                        ch.queue_purge(queue=qname)
                        logger.info(f"[Fog]: purged AMQP queue '{qname}' on boot.")
                    except Exception as e:
                        logger.warning(f"[Fog]: purge '{qname}' failed: {e}")

                logger.info(
                    "[Fog]: per-fog queue '%s' → messages=%d, consumers=%d (quorum=%s)",
                    declare_result.method.queue,
                    getattr(declare_result.method, 'message_count', -1),
                    getattr(declare_result.method, 'consumer_count', -1),
                    bool(queue_args),
                )

                ch.basic_qos(prefetch_count=1)

                def on_model(_ch, method, _props, body):
                    try:
                        msg = json.loads(body.decode("utf-8"))
                    except Exception as e:
                        logger.warning(f"[Fog]: invalid cloud AMQP payload: {e}")
                        _ch.basic_ack(delivery_tag=method.delivery_tag); return

                    if msg.get("command") == "2" or "model" in msg:
                        if not st.outbox_enabled:
                            logger.info(f"[Fog]: enabling outbox worker (cloud model broadcast).")
                        st.enable_outbox(True)

                    if msg.get("model"):
                        try:
                            os.makedirs(os.path.dirname(FogResourcesPaths.FOG_MODEL_FILE_PATH.value), exist_ok=True)
                            with open(FogResourcesPaths.FOG_MODEL_FILE_PATH.value, "wb") as f:
                                import base64
                                f.write(base64.b64decode(msg["model"]))
                                f.flush(); os.fsync(f.fileno())
                            logger.info(f"[Fog]: (AMQP) updated fog model from cloud broadcast.")
                            self.event_bus.publish(
                                topic="fog/events/cloud-model-downlink",
                                payload={"round_id": msg.get("round_id"), "ts": int(time.time())}
                            )
                        except Exception as e:
                            logger.exception(f"[Fog]: failed writing fog model: {e}")

                    rid = msg.get("round_id")
                    if rid is not None and rid != st.round_id:
                        logger.info(f"[Fog]: new round_id={rid} (was {st.round_id}) → outbox will be pruned by worker.")
                        st.persist(rid)

                    try:
                        with pika.BlockingConnection(pika.ConnectionParameters(host=cfg.fog_amqp_host, heartbeat=30,
                                                                              blocked_connection_timeout=60,
                                                                              client_properties={"connection_name": f"fog:{fog_name}:edge-forwarder"})) as lc:
                            lch = lc.channel()
                            for edge in getattr(FederatedNodeState.get_current_node(), "child_nodes", []) or []:
                                edge_q = f"edge_{edge.name}_messages_queue"
                                lch.queue_declare(queue=edge_q, durable=True, auto_delete=False)
                                lch.basic_publish(exchange='', routing_key=edge_q,
                                                  body=json.dumps(msg).encode('utf-8'),
                                                  properties=pika.BasicProperties(delivery_mode=2, content_type="application/json"))
                        logger.info(f"[Fog]: (AMQP) forwarded cloud message to local edges.")
                    except Exception as e:
                        logger.exception(f"[Fog]: failed forwarding to edges: {e}")

                    _ch.basic_ack(delivery_tag=method.delivery_tag)

                ch.basic_consume(queue=qname, on_message_callback=on_model, auto_ack=False)
                logger.info(f"[Fog]: consuming cloud messages from '{qname}'.")

                if announced_down:
                    logger.info(f"[Fog]: cloud AMQP back online; bridge reconnected.")
                    announced_down = False; delay = 5

                ch.start_consuming()

            except (socket.gaierror, pika.exceptions.AMQPError) as e:
                now = time.time()
                if not announced_down:
                    logger.warning(f"[Fog]: cloud AMQP unavailable ({e.__class__.__name__}). Retrying up to {max_delay}...")
                    announced_down = True; next_warn = now + 30
                time.sleep(delay + random.uniform(0, 1.0)); delay = min(delay * 2, max_delay)
            except Exception:
                logger.exception(f"[Fog]: unexpected error in cloud AMQP bridge; retrying in {delay}...")
                time.sleep(delay); delay = min(delay * 2, max_delay)
            finally:
                try:
                    if conn and conn.is_open: conn.close()
                except Exception: pass