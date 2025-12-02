# cloud/communication/commands.py
import time
from typing import Dict, Optional
from cloud.communication.mqtt import MqttPublisher
from cloud.communication.state import RoundState
from shared.logging_config import logger
from cloud.communication.cloud_commands import CloudEnvelope

class CloudGateway:
    def __init__(self, pub: MqttPublisher, state: RoundState):
        self.pub = pub
        self.state = state

    def _publish_cloud_to_fog(self, env: CloudEnvelope) -> None:
        # Targeted vs broadcast
        if env.target and env.target.lower() != "all":
            topic = f"cloud/agent/fog/{env.target}/commands"
        else:
            topic = "cloud/fog/command"
        self.pub.publish(topic, env.to_dict(), qos=env.qos or 1, retain=False)

    def notify_create_local_model(self, target: Optional[str] = None):
        env = CloudEnvelope.make(
            cmd="CREATE_LOCAL_MODEL",
            origin="[cloud]",
            target=target,
            payload={"reason": "bootstrap"},
        )
        self._publish_cloud_to_fog(env)
        if target:
            logger.info("Cloud (MQTT): sent CREATE_LOCAL_MODEL to %s", target)
        else:
            logger.info("Cloud (MQTT): sent CREATE_LOCAL_MODEL to all fogs (broadcast).")

    def notify_start_first_training(self, data: Dict, target: Optional[str] = None):
        round_id = data.get("round_id", int(time.time() * 1000))
        date = data.get("date")
        self.state.persist(round_id, date=date)
        # Agent-driven orchestration: only publish the event; cloud-agent will target a fog cluster via PLAN_ROUND.
        self.pub.publish(
            "cloud/events/round-started",
            {"round_id": round_id, "date": date, "data": data, "ts": int(time.time())},
            qos=1, retain=False
        )
        logger.info("Cloud: started round %s date %s (agent-driven)", round_id, date)
