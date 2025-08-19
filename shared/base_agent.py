import threading
import time
from abc import ABC
from typing import Optional
from shared.logging_config import logger

class Agent(ABC):
    """
    Minimal agent interface. Agents run a lighweight loop in a daemon thread and can react to events raised by the
    messaging layer via hooks.
    """

    def __init__(self, name: str):
        self.name = name
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()

    # -------------- lifecycle --------------------
    def start(self):
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True, name=f"{self.name}-agent")
        self._thread.start()
        logger.info(f"Agent {self.name} started...")

    def stop(self):
        self._stop.set()
        logger.info(f"Agent {self.name} stopped...")

    def _run(self):
        # main tick loop - override tick() for periodic tasks
        while not self._stop.is_set():
            try:
                self.tick()
            except Exception as e:
                logger.warning(f"Agent {self.name} tick error: {e}")
            time.sleep(self.tick_interval_seconds())

    def tick_interval_seconds(self) -> float:
        return 2.0

    def tick(self):
        """Periodic no-op. Override in subclasses if needed."""
        pass

    # -------------- generic hooks ---------------------
    def on_round_started(self, round_id: int, data: dict): pass
    def on_cloud_model_broadcast(self, round_id: int, data: dict): pass
    def on_fog_model_received(self, fog_name: str, round_id: int, path: str): pass
    def on_edge_model_received(self, edge_key: str, metrics: dict, path: str): pass
    def on_command(self, topic: str, payload: dict): pass