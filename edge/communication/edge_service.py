import base64
import os.path
from shared.logging_config import logger
from edge.communication.edge_resources_paths import EdgeResourcesPaths
from edge.model.model_architectures import create_model
from edge.model.model_training_service import train_local_edge_model
from edge.communication.edge_messaging import EdgeMessaging


class EdgeService:

    def __init__(self, messaging: EdgeMessaging):
        self.edge_messaging = messaging

    @staticmethod
    def create_local_edge_model():
        # adapt to provide model reference for creation
        edge_model = create_model('simple_lstm_two_gates')

        local_edge_model_path = os.path.join(
            EdgeResourcesPaths.MODELS_FOLDER_PATH,
            EdgeResourcesPaths.NON_TRAINED_LOCAL_EDGE_MODEL_FILE_PATH
        )
        edge_model.save(local_edge_model_path)
        logger.info("Successfully created and saved local edge model.")

    def train_edge_local_model(self, payload):
        # placeholder date
        date = payload.get('data', {}).get('date')
        metrics = train_local_edge_model(date)
        # complete with fog host
        self.edge_messaging.send_trained_model(EdgeResourcesPaths.TRAINED_LOCAL_EDGE_MODEL_FILE_PATH, metrics)

    def retrain_fog_model(self, msg):
        local_edge_model_path = os.path.join(
            EdgeResourcesPaths.MODELS_FOLDER_PATH,
            EdgeResourcesPaths.NON_TRAINED_LOCAL_EDGE_MODEL_FILE_PATH
        )

        model_bytes = base64.b64decode(msg['model'])
        with open(local_edge_model_path, "wb") as f:
            f.write(model_bytes)

        date = msg.get('data', {}).get('date')
        metrics = train_local_edge_model(date)
        # complete with fog host
        self.edge_messaging.send_trained_model(EdgeResourcesPaths.TRAINED_LOCAL_EDGE_MODEL_FILE_PATH, metrics)
