from shared.base_agent import Agent
from shared.logging_config import logger

class FogAgent(Agent):
    def __init__(self):
        super().__init__("fog")
        self.messaging = None

    def attach_messaging(self, messaging):
        self.messaging = messaging

    def tick(self):
        # example, push status heartbeat upstream or nudge edges when stale
        pass

    # hooks
    def on_command(self, topic: str, payload: dict):
        logger.info(f"[FogAgent] Command on {topic}: {payload}")

    def on_edge_model_received(self, edge_key: str, metric: dict, path: str):
        logger.info(f"[FogAgent] Edge model with key {edge_key} received at {path} with metrics {metric}")
