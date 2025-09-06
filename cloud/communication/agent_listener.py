import json
import paho.mqtt.client as mqtt
from shared.logging_config import logger

class AgentCommandListener:
    def __init__(self, cfg, state, pub):
        self.cfg, self.state, self.pub = cfg, state, pub

    def start(self):
        def on_connect(c, _u, _f, rc):
            if rc == 0:
                logger.info("[Cloud]: MQTT: connected (agent listener).")
                c.subscribe("cloud/agent/commands", qos=1)
            else:
                logger.error(f"[Cloud]: MQTT: connect failed (rc={rc}).")

        def on_message(_c, _u, msg):
            try:
                data = json.loads(msg.payload.decode("utf-8"))
            except Exception:
                return
            cmd = str(data.get("cmd", "")).upper()
            if cmd == "GLOBAL_THROTTLE":
                rate = float(data.get("rate", 0))
                self.state.global_throttle = rate
                self.pub.publish("cloud/fog/command",
                                 {"command": "GLOBAL_THROTTLE", "rate": rate})
            elif cmd == "SELECT_FOG":
                self.state.selected_fog = data.get("target")

        client = mqtt.Client()
        client.on_connect = on_connect
        client.on_message = on_message
        client.connect(self.cfg.cloud_mqtt_host, self.cfg.cloud_mqtt_port)
        client.loop_forever()
