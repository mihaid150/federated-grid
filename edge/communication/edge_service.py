import base64
from pathlib import Path
from typing import TYPE_CHECKING
from shared.logging_config import logger
from edge.communication.edge_resources_paths import EdgeResourcesPaths
from edge.model.model_architectures import create_model
from edge.model.model_training_service import train_local_edge_model

if TYPE_CHECKING:
    from edge.communication.edge_messaging import EdgeMessaging

class EdgeService:

    def __init__(self, messaging: 'EdgeMessaging'):
        self.edge_messaging = messaging

    # ---------- called by fog commands (MQTT cmd 0) ----------
    @staticmethod
    def create_local_edge_model():
        edge_model = create_model('simple_lstm_two_gates')

        # ensure dir exists
        Path(EdgeResourcesPaths.MODELS_FOLDER_PATH.value).mkdir(parents=True, exist_ok=True)

        # use the full path constant directly
        local_edge_model_path = EdgeResourcesPaths.NON_TRAINED_LOCAL_EDGE_MODEL_FILE_PATH.value

        edge_model.save(local_edge_model_path)
        logger.info("Successfully created and saved local edge model.")

    # ---------- called by fog commands (MQTT cmd 1) ----------
    def train_edge_local_model(self, payload):
        # placeholder date
        date = payload.get('data', {}).get('date')
        metrics = train_local_edge_model(date)
        # complete with fog host
        local_edge_model_path = EdgeResourcesPaths.TRAINED_LOCAL_EDGE_MODEL_FILE_PATH.value
        self.edge_messaging.send_trained_model(local_edge_model_path, metrics)

    # ---------- called by fog AMQP cmd 2 (broadcast with model) ----------
    def retrain_fog_model(self, msg):
        local_edge_model_path = EdgeResourcesPaths.NON_TRAINED_LOCAL_EDGE_MODEL_FILE_PATH.value

        model_bytes = base64.b64decode(msg['model'])
        with open(local_edge_model_path, "wb") as f:
            f.write(model_bytes)

        date = msg.get('data', {}).get('date')
        metrics = train_local_edge_model(date)
        # complete with fog host
        trained_edge_model_file_path = EdgeResourcesPaths.TRAINED_LOCAL_EDGE_MODEL_FILE_PATH.value
        self.edge_messaging.send_trained_model(trained_edge_model_file_path, metrics)

    # ---------- called by agent nudges over MQTT (agent/<edge>/commands) ----------
    def handle_agent_nudge(self, nudge: dict):
        """
        Expected schema (from federated-agents/common/contracts.NudgeCommand):
          {
            "command": "REQUEST_RETRAIN",
            "reason": "drift",
            "mae": <float>,
            "params": { ... }
          }
        """
        cmd = (nudge or {}).get("command", "").upper()
        params = (nudge or {}).get("params", {}) or {}

        if cmd == "REQUEST_RETRAIN":
            logger.info("[edge-service] agent requested retrain (reason=%s, mae=%s, params=%s)",
                        nudge.get("reason"), nudge.get("mae"), params)
            # You can pass params into your training logic as needed (e.g., window_days)
            self.train_edge_local_model({"data": params})
        else:
            logger.info("[edge-service] ignoring agent command: %r", cmd)
