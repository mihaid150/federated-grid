import os.path
from shared.logging_config import logger
from edge.edge_resources_paths import EdgeResourcesPaths
from edge.model_architectures import create_model
from edge.model_training_service import train_local_edge_model


class EdgeService:

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

    @staticmethod
    def train_edge_local_model(msg):
        # placeholder date
        date = msg.get('date')
        metrics = train_local_edge_model(date)