import threading
import paho.mqtt.client as mqtt
from shared.logging_config import logger
from shared.node_state import FederatedNodeState


class EdgeRuntimeState:
    def __init__(self):
        self.last_cmd_id = None
        self._pub_client: mqtt.Client | None = None
        self._pub_lock = threading.Lock()


    # lazy, shared telemetry publisher
    def ensure_pub_client(self, host: str, port: int) -> mqtt.Client:
        with self._pub_lock:
            if self._pub_client is None:
                c = mqtt.Client(client_id=f"edge-pub-{FederatedNodeState.get_current_node().name}", clean_session=True)
                c.reconnect_delay_set(min_delay=1, max_delay=30)
                c.connect(host, port)
                import threading as _t
                _t.Thread(target=c.loop_forever, daemon=True).start()
                self._pub_client = c
        return self._pub_client