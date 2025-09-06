import threading
from shared.logging_config import logger
from edge.communication.config import EdgeConfig
from edge.communication.state import EdgeRuntimeState
from edge.communication.mqtt_control import MqttControl
from edge.communication.amqp_control import AmqpControl
from edge.communication.uploader import ModelUploader
from edge.communication.telemetry import Telemetry


class EdgeCoordinator:
    def __init__(self, cfg: EdgeConfig | None = None):
        self.cfg = cfg or EdgeConfig()
        self.state = EdgeRuntimeState()
        self.edge_service = None
        # components constructed later after service is attached
        self._mqtt = None
        self._amqp = None
        self.uploader = ModelUploader(self.cfg.fog_amqp_host)
        self.telemetry = Telemetry(self.cfg.fog_mqtt_host, self.cfg.fog_mqtt_port, self.state)


    def attach_service(self, svc):
        self.edge_service = svc
        self._mqtt = MqttControl(self.cfg, self.state, svc)
        self._amqp = AmqpControl(self.cfg, svc)


    def start_background_consumers(self):
        if not self.edge_service:
            raise RuntimeError("EdgeCoordinator.attach_service must be called before starting consumers.")
        threading.Thread(target=self._mqtt.start, daemon=True).start()
        threading.Thread(target=self._amqp.start, daemon=True).start()
        logger.info(f"[Edge]: background consumers started.")


    # Back-compat helpers so EdgeService can call through the old API names if needed
    def send_trained_model(self, model_path: str, metrics: dict) -> None:
        self.uploader.send_trained_model(model_path, metrics)