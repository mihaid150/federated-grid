from enum import Enum

class CloudResourcesPaths(str, Enum):
    MODELS_FOLDER_PATH = "/app/models/"
    CLOUD_MODEL_FILE_PATH = MODELS_FOLDER_PATH + "cloud_model.keras"

    STATUS_FOLDER_PATH = "/app/status/"
    ROUND_FILE_PATH = STATUS_FOLDER_PATH + "round.json"


