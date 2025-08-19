from shared.base_agent import Agent
from shared.logging_config import logger


class CloudAgent(Agent):
    """
    Node-level wrapper that couples the running cloud node with the reusable agents design for cloud. Keeps handles
    to the Ray actors and reacts to messaging hooks from CloudMessaging.
    """
    def __init__(self):
        super().__init__("cloud")
        self.messaging = None

        # ray actors handles
        self._orchestator = None
        self._bandit = None

    # ------ wiring -----------------
    def attach_messaging(self, messaging):
        self.messaging = messaging

    def tick(self):
        # example, monitor all fogs reported in recent window or drive next round
        pass

    # hooks from messaging
    def on_round_started(self, round_id: int, data: dict):
        logger.info(f"[CloudAgent] Round {round_id} started with data {data}.")

    def on_fog_model_received(self, fog_name: str, round_id: int, path: str):
        logger.info(f"[CloudAgent] Received fog model from {fog_name} for round {round_id} at {path}.")

    def on_cloud_model_broadcast(self, round_id: int, data: dict):
        logger.info(f"[CloudAgent] Cloud model broadcast for round {round_id}.")