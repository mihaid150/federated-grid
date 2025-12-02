import json, time
import paho.mqtt.client as mqtt
from shared.logging_config import logger

class MqttPublisher:
    def __init__(self, host: str, port: int, client_id: str = "cloud-publisher"):
        self.host, self.port, self.client_id = host, port, client_id

    def publish(self, topic: str, message: dict, qos: int = 1, retain: bool = True, retries: int = 10, delay: int = 5):
        cli = mqtt.Client(client_id=self.client_id, clean_session=True)
        for attempt in range(1, retries + 1):
            try:
                cli.connect(self.host, self.port)
                break
            except Exception as e:
                logger.warning(f"[Cloud]: MQTT connect failed ({attempt}/{retries}): {e}")
                if attempt == retries:
                    return
                time.sleep(delay)
        cli.publish(topic, json.dumps(message), qos=qos, retain=retain)
        cli.disconnect()

    def clear_retained(self, topic: str, qos: int = 1):
        cli = mqtt.Client(client_id=f"{self.client_id}-clear-{int(time.time()*1000)}", clean_session=True)
        try:
            cli.connect(self.host, self.port)
            # Empty payload + retain=True clears the retained message
            cli.publish(topic, b"", qos=qos, retain=True)
        finally:
            try: cli.disconnect()
            except: pass