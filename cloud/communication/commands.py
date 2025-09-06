import time
from typing import Dict

from cloud.communication.mqtt import MqttPublisher
from cloud.communication.state import RoundState
from shared.logging_config import logger


class Commands:
    def __init__(self, pub: MqttPublisher, state: RoundState):
        self.pub = pub
        self.state = state

    def notify_create_local_model(self):
        cmd = {"command": "0", "cmd_id": int(time.time() * 1000)}
        self.pub.publish("cloud/fog/command", cmd, qos=1, retain=False)
        logger.info("Cloud (MQTT): sent create-local-model to fogs (retain=False).")

    def notify_start_first_training(self, data: Dict):
        round_id = data.get("round_id", int(time.time() * 1000))
        self.state.persist(round_id)
        cmd = {"command": "1", "cmd_id": int(time.time() * 1000), "round_id": round_id, "data": data}
        self.pub.publish("cloud/fog/command", cmd, qos=1, retain=False)
        self.pub.publish("cloud/events/round-started",
                         {"round_id": round_id, "data": data, "ts": int(time.time())}, qos=1, retain=False)
        logger.info("Cloud (MQTT): started round %s", round_id)