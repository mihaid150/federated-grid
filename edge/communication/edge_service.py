import base64
from pathlib import Path
from typing import TYPE_CHECKING
from shared.logging_config import logger
from edge.communication.edge_resources_paths import EdgeResourcesPaths
from edge.model.model_architectures import create_model
from edge.model.model_training_service import train_local_edge_model


if TYPE_CHECKING:
    from edge.communication.coordinator import EdgeCoordinator


class EdgeService:
    def __init__(self, coordinator: 'EdgeCoordinator'):
        self.edge_coordinator = coordinator


    @staticmethod
    def create_local_edge_model():
        edge_model = create_model('simple_lstm_two_gates')
        Path(EdgeResourcesPaths.MODELS_FOLDER_PATH.value).mkdir(parents=True, exist_ok=True)
        local_edge_model_path = EdgeResourcesPaths.NON_TRAINED_LOCAL_EDGE_MODEL_FILE_PATH.value
        edge_model.save(local_edge_model_path)
        logger.info(f"[Edge]: Successfully created and saved local edge model.")


    def train_edge_local_model(self, payload):
        date = payload.get('data', {}).get('date') if isinstance(payload, dict) else None
        metrics = train_local_edge_model(date)
        local_edge_model_path = EdgeResourcesPaths.TRAINED_LOCAL_EDGE_MODEL_FILE_PATH.value
        self.edge_coordinator.send_trained_model(local_edge_model_path, metrics)


    def retrain_fog_model(self, msg):
        local_edge_model_path = EdgeResourcesPaths.NON_TRAINED_LOCAL_EDGE_MODEL_FILE_PATH.value
        model_bytes = base64.b64decode(msg['model'])
        with open(local_edge_model_path, "wb") as f:
            f.write(model_bytes)
        date = msg.get('data', {}).get('date') if isinstance(msg, dict) else None
        metrics = train_local_edge_model(date)
        trained_edge_model_file_path = EdgeResourcesPaths.TRAINED_LOCAL_EDGE_MODEL_FILE_PATH.value
        self.edge_coordinator.send_trained_model(trained_edge_model_file_path, metrics)


    def handle_agent_nudge(self, nudge: dict):
        cmd = (nudge or {}).get("command", "").upper()
        params = (nudge or {}).get("params", {}) or {}
        if cmd == "REQUEST_RETRAIN":
            logger.info("[Edge]: agent requested retrain (reason=%s, mae=%s, params=%s)",
            nudge.get("reason"), nudge.get("mae"), params)
            self.train_edge_local_model({"data": params})
        else:
            logger.info(f"[Edge]: ignoring agent command: {cmd}")