import os, time
import ray
from ray import serve
from agents.edge.sensing_agent import SensingAgent
from agents.edge.forecast_serve import ForecasterAgent
from agents.common.adapters.mqtt_bridge import MQTTBridge

EDGE_NAME = os.getenv("EDGE_NAME", os.getenv("NODE_NAME", "edge1_fog1"))

if __name__ == "__main__":
    # Start local Ray runtime
    ray.init(ignore_reinit_error=True)

    # Deploy Serve app (forecast endpoint)
    serve.start(detached=True)
    serve.run(ForecasterAgent.bind())

    # Create SensingAgent
    sensing = SensingAgent.options(name=f"sensing_{EDGE_NAME}", lifetime_scope="detached").remote()

    # Start MQTT bridge
    bridge = MQTTBridge(sensing)
    bridge.start()

    print(f"[edge-agent] {EDGE_NAME} online. Ray and MQTT running.")
    while True:
        time.sleep(60)