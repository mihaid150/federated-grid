import json, time
import paho.mqtt.client as mqtt
from shared.logging_config import logger

class FogEventBus:
    def __init__(self, host: str, port: str):
        self.host = host
        self.port = port

    def publish(self, topic: str, payload: dict, qos: int = 1, retain: bool = False):
        try:
            client = mqtt.Client(client_id=f"fog-events-{int(time.time() * 1000)}", clean_session=True)
            client.connect(self.host, self.port)
            client.publish(topic, json.dumps(payload), qos=qos, retain=retain)
            client.disconnect()
            logger.info(f"[Fog]: (Event) Published topic {topic} with payload {payload}.")
        except Exception as e:
            logger.warning(f"[Fog]: (Event) Failed to publish topic {topic} with payload {payload}: {e}")