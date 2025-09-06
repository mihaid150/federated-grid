import base64, json, os, time, random, socket
import pika
from shared.logging_config import logger
from shared.node_state import FederatedNodeState
from fog.communication.config import FogConfig
from fog.communication.state import FogRoundState
from fog.communication.fog_resources_paths import FogResourcesPaths

class EdgeModelIngestor:
    def __init__(self, cfg: FogConfig, state: FogRoundState, event_bus):
        self.cfg, self.state, self.event_bus = cfg, state, event_bus
        self.edge_models_cache: dict[str, dict] = {}

    def start(self, on_all_ready):
        cfg, st = self.cfg, self.state
        delay, max_delay = 5, 60
        announced_down = False

        while True:
            conn = None
            try:
                fog_name = FederatedNodeState.get_current_node().name
                params = pika.ConnectionParameters(host=cfg.fog_amqp_host, heartbeat=30, blocked_connection_timeout=60,
                                                   client_properties={"connection_name": f"fog:{fog_name}:edge-consumer"})
                conn = pika.BlockingConnection(params)
                ch = conn.channel()
                ch.queue_declare(queue='edge_to_fog_models', durable=True, auto_delete=False)
                if cfg.purge_edge_models_queue_on_boot:
                    try:
                        ch.queue_purge(queue='edge_to_fog_models')
                        logger.info(f"[Fog]: purged 'edge_to_fog_models' on boot.")
                    except Exception as e:
                        logger.warning(f"[Fog]: failed to purge 'edge_to_fog_models': {e}")
                ch.basic_qos(prefetch_count=1)

                def on_edge(ch_, method, _props, body):
                    try:
                        payload = json.loads(body)
                        edge_mac = payload['edge_mac']; edge_name = payload['edge_name']
                        model_bytes = base64.b64decode(payload['model'])
                        model_path = os.path.join(FogResourcesPaths.MODELS_FOLDER_PATH.value, f"{edge_name}_trained_model.keras")
                        os.makedirs(os.path.dirname(model_path), exist_ok=True)
                        with open(model_path, "wb") as f:
                            f.write(model_bytes); f.flush(); os.fsync(f.fileno())
                        metrics = payload['metrics']
                        key = f"{edge_mac}_{edge_name}"
                        self.edge_models_cache[key] = {"model_path": model_path, "metrics": metrics}
                        logger.info(f"[Fog]: cached model from edge {edge_name} with metrics {metrics}")

                        self.event_bus.publish(
                            topic="fog/events/edge-model-received",
                            payload={"round_id": st.round_id, "edge_name": edge_name, "model_path": model_path, "metrics": metrics, "ts": int(time.time())}
                        )
                    except Exception:
                        logger.exception(f"[Fog]: failed handling edge model.")
                    finally:
                        ch_.basic_ack(delivery_tag=method.delivery_tag)
                        if not st.outbox_enabled:
                            logger.info(f"[Fog]: enabling outbox worker (received edge model).")
                            st.enable_outbox(True)
                        on_all_ready(self.edge_models_cache)

                ch.basic_consume(queue='edge_to_fog_models', on_message_callback=on_edge, auto_ack=False)
                logger.info(f"[Fog]: listening for trained models from edges...")
                if announced_down:
                    logger.info(f"[Fog]: local AMQP back online; edge ingestor reconnected.")
                    announced_down = False
                delay = 5
                ch.start_consuming()

            except (socket.gaierror, pika.exceptions.AMQPError):
                time.sleep(delay + random.uniform(0, 1.0)); delay = min(delay * 2, max_delay)
            except Exception:
                logger.exception(f"[Fog]: unexpected error in edge ingestor; retrying in {delay}...")
                time.sleep(delay); delay = min(delay * 2, max_delay)
            finally:
                try:
                    if conn and conn.is_open: conn.close()
                except Exception: pass