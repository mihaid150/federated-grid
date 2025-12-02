import time

import pika
import base64, json, os
from cloud.communication.amqp import AmqpClient
from cloud.communication.cloud_resources_paths import CloudResourcesPaths
from shared.logging_config import logger
from shared.node_state import FederatedNodeState

class Broadcaster:
    def __init__(self, cfg, state, pub):
        self.cfg, self.state, self.pub = cfg, state, pub

    def _ensure_per_fog_queues_and_publish(self, ch, message_body: bytes):
        node = FederatedNodeState.get_current_node()
        fogs = getattr(node, "child_nodes", []) or []
        if not fogs:
            logger.warning("[Cloud]: no fog nodes registered; nothing to broadcast to.")
            return
        for fog in fogs:
            q = f"cloud_fanout_for_{fog.name}"
            ch.queue_declare(queue=q, durable=True, auto_delete=False)
            ch.basic_publish(
                exchange='',
                routing_key=q,
                body=message_body,
                properties=pika.BasicProperties(delivery_mode=2),
                mandatory=True,
            )
            logger.info(f"[Cloud]: (AMQP): enqueued model for fog '{fog.name}' in '{q}'.")

    def broadcast_model(self, data: dict):
        round_id = data.get("round_id")
        self.state.persist(round_id)
        model_path = CloudResourcesPaths.CLOUD_MODEL_FILE_PATH.value
        if not os.path.exists(model_path):
            logger.error(f"[Cloud]: no aggregated model at {model_path}; cannot broadcast.")
            return
        with open(model_path, "rb") as f:
            model_b64 = base64.b64encode(f.read()).decode("utf-8")
        body = json.dumps({"command": "2", "round_id": round_id, "model": model_b64, "data": data}).encode("utf-8")

        conn = AmqpClient(self.cfg.cloud_amqp_host).open_blocking()
        try:
            ch = conn.channel()
            self._ensure_per_fog_queues_and_publish(ch, body)
            logger.info("[Cloud]: (AMQP): broadcast cloud model to per-fog queues.")
        finally:
            try: conn.close()
            except Exception: pass

        # optional agent event
        try:
            self.pub.publish("cloud/events/cloud-model-broadcast", {"round_id": round_id, "ts": int(time.time())},
                             qos=1, retain=False)
        except Exception as e:
            logger.warning("Cloud: failed to publish cloud-model-broadcast: %s", e)
