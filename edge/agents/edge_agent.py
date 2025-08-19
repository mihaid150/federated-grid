from typing import Dict
from shared.base_agent import Agent
from shared.logging_config import logger

class EdgeAgent(Agent):
    def __init__(self):
        super().__init__("edge")
        self.messaging = None

    def attach_messaging(self, messaging):
        self.messaging = messaging

    def tick(self):
        # example, lightweight local telemetry/feature curation
        pass

    # hooks
    def on_command(self, topic: str, payload: Dict):
        logger.info(f"[EdgeAgent] Received command {topic}: {payload}")