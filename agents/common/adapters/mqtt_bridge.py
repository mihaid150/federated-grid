import os, json, threading
import paho.mqtt.client as mqtt
import ray
from agents.common.contracts import Telemetry, NudgeCommand

EDGE_NAME = os.getenv("EDGE_NAME", os.getenv("NODE_NAME", "edge1_fog1"))
MQTT_HOST = os.getenv("FOG_MQTT_HOST", "mqtt-fog1")
MQTT_PORT = int(os.getenv("FOG_MQTT_PORT", 1883))

INPUT_TOPIC = os.getenv("MQTT_INPUT_TOPIC", f"node/{EDGE_NAME}/telemetry")
OUTPUT_TOPIC = os.getenv("MQTT_OUTPUT_TOPIC", f"agent/{EDGE_NAME}/commands")

# Handles are passed in at init time
class MQTTBridge:
    def __init__(self, sensing_handle, policy_threshold: float = 0.12):
        self.sensing = sensing_handle
        self.policy_threshold = policy_threshold
        self.cli = mqtt.Client()
        self.cli.on_connect = self._on_connect
        self.cli.on_message = self._on_message

    def start(self):
        self.cli.connect(MQTT_HOST, MQTT_PORT)
        threading.Thread(target=self.cli.loop_forever, daemon=True).start()

    def _on_connect(self, client, userdata, flags, rc):
        client.subscribe(INPUT_TOPIC, qos=0)
        print(f"[mqtt_bridge] Subscribed to {INPUT_TOPIC}")

    def _on_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode())
            t = Telemetry(**payload)
        except Exception as e:
            print("[mqtt_bridge] Bad payload:", e)
            return
        # Send to sensing agent. It self-predicts using online model.
        fut = self.sensing.observe.remote(t.ts, t.value)
        metrics = ray.get(fut)  # {drift, mae, n}
        # Decide and publish a nudge if needed
        should = ray.get(self.sensing.should_nudge.remote(self.policy_threshold, True))
        if should:
            cmd = NudgeCommand(command="REQUEST_RETRAIN", reason="drift", mae=metrics["mae"], params={"window_days": 2})
            self.cli.publish(OUTPUT_TOPIC, cmd.model_dump_json(), qos=1)
            print(f"[mqtt_bridge] Nudge published to {OUTPUT_TOPIC}: {cmd}")