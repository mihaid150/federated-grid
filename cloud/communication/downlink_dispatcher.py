# cloud/communication/downlink_dispatcher.py
import time
import pika
import base64, json, os
from cloud.communication.amqp import AmqpClient
from cloud.communication.cloud_resources_paths import CloudResourcesPaths
from shared.logging_config import logger
from shared.node_state import FederatedNodeState
from cloud.communication.cloud_commands import CloudEnvelope

class DownlinkDispatcher:
    def __init__(self, cfg, state, pub):
        self.cfg, self.state, self.pub = cfg, state, pub

    def _ensure_per_fog_queues_and_publish(self, ch, message_body: bytes, targets: list[str] | None = None):
        node = FederatedNodeState.get_current_node()
        # if specific targets provided, use them; else default to all node child fogs
        if targets:
            names = [str(t) for t in targets]
        else:
            fogs = getattr(node, "child_nodes", []) or []
            names = [fog.name for fog in fogs]
        if not names:
            logger.warning("[Cloud]: no fog targets to dispatch to.")
            return
        for name in names:
            q = f"cloud_fanout_for_{name}"
            ch.queue_declare(queue=q, durable=True, auto_delete=False)
            ch.basic_publish(
                exchange='',
                routing_key=q,
                body=message_body,
                properties=pika.BasicProperties(delivery_mode=2, content_type="application/json"),
                mandatory=True,
            )
            logger.info("[Cloud]: (AMQP): enqueued model for fog '%s' in '%s'.", name, q)

    def downlink_dispatch_model(self, data: dict):
        round_id = data.get("round_id")
        self.state.persist(round_id)
        model_path = CloudResourcesPaths.CLOUD_MODEL_FILE_PATH.value
        if not os.path.exists(model_path):
            logger.error("[Cloud]: no aggregated model at %s; cannot dispatch.", model_path)
            return

        with open(model_path, "rb") as f:
            model_b64 = base64.b64encode(f.read()).decode("utf-8")

        # New envelope (command record) in AMQP payload
        env = CloudEnvelope.make(
            cmd="CLOUD_MODEL_DISPATCH",
            origin="[cloud]",
            target="all",
            round_id=round_id,
            payload={"model": model_b64, "data": data}
        )
        body = json.dumps(env.to_dict()).encode("utf-8")

        conn = AmqpClient(self.cfg.cloud_amqp_host).open_blocking()
        try:
            ch = conn.channel()
            # Determine targets: prefer explicit in state.downlink_fogs; else in 'data.targets'/'data.target'; else all
            targets = getattr(self.state, 'downlink_fogs', None)
            if not targets:
                possible = data.get('targets') or []
                if isinstance(possible, list) and possible:
                    targets = [str(t) for t in possible]
                else:
                    tgt = data.get('target')
                    if tgt and str(tgt).lower() != 'all':
                        targets = [str(tgt)]
            # Persist expected fogs for the next aggregation wave
            try:
                if targets:
                    self.state.set_expected_fogs(list(targets))
                else:
                    self.state.set_expected_fogs(None)
            except Exception:
                pass
            self._ensure_per_fog_queues_and_publish(ch, body, targets=targets)
            logger.info("[Cloud]: (AMQP): dispatch cloud model to %s", targets or 'all fogs')
        finally:
            try: conn.close()
            except Exception: pass

        # optional event for dashboards/agents
        try:
            self.pub.publish(
                "cloud/events/cloud-model-dispatch",
                {"round_id": round_id, "ts": int(time.time())},
                qos=1, retain=False
            )
        except Exception as e:
            logger.warning("Cloud: failed to publish cloud-model-dispatch: %s", e)
