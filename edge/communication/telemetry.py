import json
from shared.logging_config import logger
from shared.node_state import FederatedNodeState
from edge.communication.state import EdgeRuntimeState


class Telemetry:
    def __init__(self, host: str, port: int, state: EdgeRuntimeState):
        self.host, self.port, self.state = host, port, state


    def publish(self, ts: str, value: float, type_: str = "measure", meta: dict | None = None):
        topic = f"node/{FederatedNodeState.get_current_node().name}/telemetry"
        payload = {"ts": ts, "value": float(value), "type": type_, "meta": meta or {}}
        try:
            client = self.state.ensure_pub_client(self.host, self.port)
            client.publish(topic, json.dumps(payload), qos=0, retain=False)
        except Exception as e:
            logger.warning(f"[Edge]: failed to publish telemetry to {topic}: {e}")